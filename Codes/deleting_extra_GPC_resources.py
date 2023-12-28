from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
from google.cloud.dataproc_v1.types import ListJobsRequest, DeleteJobRequest

def move_file(bucket_name, source_blob_name, destination_blob_name):
    # Initialize a client
    storage_client = storage.Client()

    # Get the source bucket and blob
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(source_blob_name)

    # Get the destination bucket and blob
    destination_bucket = storage_client.bucket(bucket_name)
    destination_blob = destination_bucket.blob(destination_blob_name)

    # Download the content of the source blob
    source_content = source_blob.download_as_text()

    # Upload the content to the destination blob
    destination_blob.upload_from_string(source_content)

    # Delete the source blob
    source_blob.delete()

def delete_buckets_with_prefix(bucket_prefix):
    storage_client = storage.Client()

    # List all buckets
    buckets = list(storage_client.list_buckets())

    # Delete buckets with the specified prefix
    for bucket in buckets:
        if bucket.name.startswith(bucket_prefix):
            bucket.delete(force=True)

def delete_all_dataproc_jobs(project_id, region):
    # Initialize a Dataproc API client
    client = dataproc.JobControllerClient(client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

    # List all jobs in the region
    jobs_request = ListJobsRequest(project_id=project_id, region=region)
    jobs = client.list_jobs(request=jobs_request)

    # Delete each job
    for job in jobs:
        job_id = job.reference.job_id
        delete_request = DeleteJobRequest(job_id=job_id, project_id=project_id, region=region)
        client.delete_job(request=delete_request)