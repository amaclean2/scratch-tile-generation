FROM --platform=linux/amd64 ghcr.io/osgeo/gdal:ubuntu-small-3.8.5 as gdal-source
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12

# Install missing library that rioxarray needs
RUN dnf update -y && \
    dnf install -y expat && \
    dnf clean all

# Copy GDAL binaries and libraries from the official GDAL image
COPY --from=gdal-source /usr/bin/gdal* /usr/bin/
COPY --from=gdal-source /usr/bin/ogr* /usr/bin/
COPY --from=gdal-source /usr/lib/x86_64-linux-gnu/libgdal* /usr/lib64/
COPY --from=gdal-source /usr/lib/x86_64-linux-gnu/libproj* /usr/lib64/
COPY --from=gdal-source /usr/lib/x86_64-linux-gnu/libgeos* /usr/lib64/
COPY --from=gdal-source /usr/lib/x86_64-linux-gnu/libexpat* /usr/lib64/
COPY --from=gdal-source /usr/share/gdal /usr/share/gdal
COPY --from=gdal-source /usr/share/proj /usr/share/proj

# Set GDAL environment variables
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV GDAL_DATA=/usr/share/gdal
ENV PROJ_LIB=/usr/share/proj

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt

COPY *.py ${LAMBDA_TASK_ROOT}/

CMD ["lambda_function.lambda_handler"]