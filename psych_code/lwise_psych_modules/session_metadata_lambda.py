import uuid
import json
import os
import traceback
import time
import datetime
import threading
import copy
import boto3


EXPERIMENT_NAME = os.environ.get('EXPERIMENT_NAME')
EXPERIMENT_NUMBER = os.environ.get('EXPERIMENT_NUMBER')
AWS_PREFIX = os.environ.get('AWS_PREFIX')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
BUCKET_URL = f"https://{BUCKET_NAME}.s3.amazonaws.com"
COUNTER_TABLE_NAME = os.environ.get('COUNTER_TABLE_NAME')
MAPPING_TABLE_NAME = os.environ.get('MAPPING_TABLE_NAME')
COMPLETION_CODE = os.environ.get('COMPLETION_CODE')
SCREEN_OUT_CODE = os.environ.get('SCREEN_OUT_CODE')

# Set up S3, and DynamoDB tables (one for keeping track of trialset_id increments, one for mapping assignment_id to trialset_id)
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
counter_table = dynamodb.Table(COUNTER_TABLE_NAME)
mapping_table = dynamodb.Table(MAPPING_TABLE_NAME)


def retrieve_trialset_id(mapping_table, assignment_id, worker_id, hit_id):
    if assignment_id is not None:
        response = mapping_table.get_item(
            Key={'assignment_id': assignment_id}
        )
        # Check if item exists and matches worker_id and hit_id (if they are provided)
        if response and 'Item' in response and \
            (worker_id is None or response['Item'].get('worker_id') == worker_id) and \
            (hit_id is None or response['Item'].get('hit_id') == hit_id) and \
            'trialset_id' in response['Item'] and response['Item']['trialset_id'] is not None:
            return int(response['Item']['trialset_id']) # Entry exists, retrieve the trialset_id
        else:
            return None
    else:
        raise ValueError(f"Must provide assignment_id (provided {assignment_id}) to retrieve trialset_id. Either provide a non-None assignment_id or provide trialset_id directly.")
    

def timestamp():
    return datetime.datetime.fromtimestamp(time.time(), tz=datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def store_trial_data(body, assignment_id):
    trial_data_key = f"trial_data/{assignment_id}/trial_{body['trial_index']}.json"
    trial_data = {key:val for key, val in body.items() if val is not None and key not in ['aws_prefix', 'experiment_name', 'experiment_number', 'request_purpose']}
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=trial_data_key,
        Body=json.dumps(trial_data),
        ContentType='application/json'
    )


def lambda_handler(event, context):
    try: 
        ### 1. SETUP

        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        # jsPsych has its own format for sending trial data - adapt to this if we are receiving trial data
        if 'submission_type' in body and body['submission_type'] in ['TRIAL', 'SESSION']:
            data_dict = json.loads(body['datastring'])
            if body['submission_type'] == 'SESSION':
                vals_source = data_dict['trials'][-1]
                vals_source['request_purpose'] = 'store_session_data_auto'
            elif body['submission_type'] == 'TRIAL':
                body = data_dict
                vals_source = body
            else:
                raise ValueError("Invalid submission_type in request body")
        else:
            vals_source = body

        keys = ['request_purpose', 'experiment_name', 'experiment_number', 'aws_prefix', 'worker_id', 'hit_id', 'platform', 'condition_idx', 'trialset_id', 'user_ip', 'user_email', 'bonus_usd', 'was_screened_out', 'datastring']            
        vals = {key: vals_source.get(key, None) for key in keys}
        assignment_id = vals_source.get('assignment_id') or str(uuid.uuid4())

        if vals['request_purpose'] == 'store_trial_data': # Store trial data asynchronously to reduce latency
            thread = threading.Thread(target=store_trial_data, args=(copy.deepcopy(body), copy.deepcopy(assignment_id)))
            thread.start()
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                    'Access-Control-Max-Age': '3600'
                },
                'body': json.dumps({'message': 'Trial data storage initiated'})  
            }

        ### 2. DETERMINE TRIALSET ID

        try:
            trialset_id = retrieve_trialset_id(mapping_table, assignment_id, vals['worker_id'], vals['hit_id']) # Check if there is already a trialset_id for this assignment

            if trialset_id is not None and vals['request_purpose'] == 'initialize_session_metadata':
                # This indicates that the page has been refreshed or the experiment otherwise restarted with the same session/assignment ID
                mapping_table.update_item(  # Increment the refresh counter
                    Key={'assignment_id': assignment_id},
                    UpdateExpression='SET refresh_count = refresh_count + :inc',
                    ExpressionAttributeValues={':inc': 1},
                    ReturnValues="UPDATED_NEW"
                )
            elif vals['request_purpose'] == 'initialize_session_metadata':
                vals['refresh_count'] = 0
        except Exception as e:
            print('WARNING: error while checking in DynamoDB table for existing trialset_id, or while incrementing refresh counter:', str(e), '... Traceback:', str(traceback.format_exc()))
            trialset_id = None
        
        if 'trialset_id' in vals and vals['trialset_id'] is not None:
            trialset_id = int(vals['trialset_id'])  # Override the value from the table
        elif trialset_id is None: # If trialset id was not in the database or the request body, draw a new one from the counter table and increment the counter
            response = counter_table.update_item(
                Key={'ID': 'uniqueKey'},
                UpdateExpression='SET CurrentCount = CurrentCount + :inc',
                ExpressionAttributeValues={':inc': 1},
                ReturnValues="UPDATED_NEW"
            )
            trialset_id = int(response['Attributes']['CurrentCount'])

        vals['trialset_id'] = trialset_id

        ### 3. STORE SESSION DATA (IF APPLICABLE)

        if vals['request_purpose'] in ["store_session_data", "store_session_data_screen_out", "store_session_data_auto"]:
            # Parse the session data from datastring (different formats for custom and auto session data storage)
            if vals['request_purpose'] in ['store_session_data', 'store_session_data_screen_out']:
                assert 'datastring' in vals and vals['datastring'], "Datastring with session data not provided"
                session_data = json.loads(vals['datastring'])
                if 'session_data' in session_data:
                    session_data = session_data['session_data']
                elif 'S' in session_data:
                    session_data = session_data['S']
            elif vals['request_purpose'] == 'store_session_data_auto':
                session_data = data_dict['trials']

            del vals['datastring']
            vals['session_data'] = session_data
            vals['data_timestamp_utc'] = timestamp()

            # Save vals to S3 as a JSON file
            key_suffix = "_auto" if vals['request_purpose'] == 'store_session_data_auto' else ""
            session_data_key = f"session_data/session_{assignment_id}{key_suffix}.json"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=session_data_key,
                Body=json.dumps(vals),
                ContentType='application/json'
            )
            vals[f"session_data_url{key_suffix}"] = f"{BUCKET_URL}/{session_data_key}"

        ### 4. UPDATE DYNAMODB TABLE

        payload = {key:val for key, val in vals.items() if val is not None and key not in ['datastring', 'session_data', 'request_purpose']}

        if vals['request_purpose'] == "initialize_session_metadata":
            payload['init_timestamp_utc'] = timestamp()

        mapping_table.update_item(
            Key={'assignment_id': assignment_id},
            UpdateExpression="SET " + ", ".join(f"{key} = :{key}" for key in payload.keys()),
            ExpressionAttributeValues={f":{key}": value for key, value in payload.items()}
        )

        ### 5. SEND RESPONSE

        response_body = {
            'assignment_id': assignment_id,
            'message': 'Data updated successfully',
        }
        if 'trialset_id' in vals:
            response_body['trialset_id'] = vals['trialset_id']
        if vals['platform'] == 'prolific': # Redirect if the participant is from Prolific
            if vals['request_purpose'] == 'store_session_data':
                response_body["redirectUrl"] = f"https://app.prolific.com/submissions/complete?cc={COMPLETION_CODE}"
            elif vals['request_purpose'] == 'store_session_data_screen_out':
                response_body["redirectUrl"] = f"https://app.prolific.com/submissions/complete?cc={SCREEN_OUT_CODE}"

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Max-Age': '3600'
            },
            'body': json.dumps(response_body)
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        print(traceback.format_exc())
        response_body = {
            'assignment_id': assignment_id,
            'message': 'An error occurred while logging data: ' + str(e),
            'traceback': str(traceback.format_exc())
        }
        if vals['platform'] == 'prolific': # Redirect if the participant is from Prolific
            if vals['request_purpose'] == 'store_session_data':
                response_body["redirectUrl"] = f"https://app.prolific.com/submissions/complete?cc={COMPLETION_CODE}"
            elif vals['request_purpose'] == 'store_session_data_screen_out':
                response_body["redirectUrl"] = f"https://app.prolific.com/submissions/complete?cc={SCREEN_OUT_CODE}"
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,POST',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Accept',
                'Access-Control-Max-Age': '3600'
            },
            'body': json.dumps(response_body)
        }
