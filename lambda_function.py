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
  """Main Lambda handler function"""
  try:
    return asyncio.run(async_main(event, context))
  except Exception as error:
    logger.error(f"Error in Lambda handler: {error}")
    return {
      'statusCode': 500,
      'body': json.dumps({'error': str(error)})
    }

async def async_main(event, context):
  try:
    kill_switch = db.lambdaControl.find_one({'action': 'stop'})
    
    if kill_switch:
      logger.info("Kill switch activated - stopping tile generation")
      return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Stopped by kill switch'})
      }
    
    current_variable = event.get('variable', 'wspd') 
    forecast_hour = event.get('forecast_hour', '01')
    
    current_timestamp = build_most_recent_file_stamp()
    current_tiles_exist = await look_for_current_tiles()
    if current_tiles_exist:
      logger.info("Current tiles already exist - skipping")
      return {'statusCode': 200, 'body': json.dumps({'status': 'skipped', 'reason': 'tiles_exist'})}
    
    files_exist = await check_for_current_weather_files()
    if not files_exist:
      logger.info("No weather files available")
      return {'statusCode': 200, 'body': json.dumps({'status': 'skipped', 'reason': 'no_files'})}
    
    result = await process_single_variable(current_variable, forecast_hour, context)
    
    next_variable = get_next_variable(current_variable)
    
    if next_variable:
      logger.info(f"Completed {current_variable}, triggering {next_variable}")
      lambda_client.invoke(
        FunctionName=context.function_name,
        InvocationType='Event',
        Payload=json.dumps({
          'variable': next_variable,
          'forecast_hour': forecast_hour
        })
      )
      
      return {
        'statusCode': 200,
        'body': json.dumps({
          'message': f'Completed {current_variable}, triggered {next_variable}',
          'tiles_generated': result.get('tiles_generated', 0)
        })
      }
    else:
      await mark_tiles_complete(current_timestamp)
      logger.info(f"All variables completed for forecast hour {forecast_hour}")
      
      return {
        'statusCode': 200,
        'body': json.dumps({
          'message': f'All variables completed for forecast hour {forecast_hour}',
          'total_tiles_generated': result.get('tiles_generated', 0)
        })
      }
        
  except Exception as error:
    logger.error(f"Error in async_main: {error}")
    raise

def get_next_variable(current_variable):
  """Get the next variable to process"""
  try:
    current_index = VARIABLES.index(current_variable)
    return VARIABLES[current_index + 1] if current_index + 1 < len(VARIABLES) else None
  except ValueError:
    return VARIABLES[0]