import json
import asyncio
import logging
import boto3
import os
from datetime import datetime, timezone

from s3_and_database_access import (
  check_for_current_weather_files, look_for_current_tiles, 
  mark_tiles_complete, db
)
from tile_processor import process_single_variable
from utils import build_most_recent_file_stamp

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda', region_name=os.getenv('AWS_REGION', 'us-east-1'))

VARIABLES = ['wspd', 'tmp', 'rh', 'MC1', 'MC10', 'MC100', 'MC1000', 'MCWOOD', 'MCHERB', 'KBDI', 'IC', 'ERC', 'BI', 'SC', 'GSI']

def lambda_handler(event, context):
  return asyncio.run(handle_step_functions_action(event, context))
    
    
async def handle_step_functions_action(event, context):
  action = event.get('action')
  override_timestamp = event.get('override_timestamp')
  
  match action:
    case 'check_kill_switch':
      kill_switch = db.lambdaControl.find_one({'action': 'stop'})
      
      return {
        'statusCode': 200,
        'kill_switch_active': bool(kill_switch)
      }
      
    case 'check_existing_tiles':
      tiles_exist = await look_for_current_tiles(override=override_timestamp)
      
      return {
        'statusCode': 200,
        'tiles_exist': tiles_exist
      }
      
    case 'check_weather_files':
      files_exist = await check_for_current_weather_files(override=override_timestamp)

      return {
        'statusCode': 200,
        'files_exist': files_exist
      }
      
    case 'process_variable':
      variable = event.get('variable', 'wspd')
      forecast_hour = event.get('forecast_hour', '01')
      result = await process_single_variable(variable, forecast_hour, context)
      
      return {
        'statusCode': 200,
        'variable': variable,
        'tiles_generated': result.get('tiles_generated', 0),
        'status': 'success'
      }
      
    case 'mark_tiles_complete':
      current_timestamp = build_most_recent_file_stamp(override=override_timestamp)
      await mark_tiles_complete(current_timestamp)
      
      return {
        'statusCode': 200,
        'timestamp': current_timestamp,
        'total_tiles_generated': event.get('total_tiles_generated', 0),
        'status': 'marked_complete'
      }
      
    case _:
      raise ValueError(f"Unknown action: {action}")