from lambda_function import lambda_handler


def return_event(key):
    return {
        'Records': [
            {
                "s3": {
                    "bucket": {
                        "name": "bicmsystems-s3-poc"
                    },
                    "object": {
                        "key": key
                    }
                }
            }
        ]
    }


event = return_event("EEVC-2021-06-23.TXT")
lambda_handler(event, None)

event = return_event("EEVD-2021-06-23.TXT")
lambda_handler(event, None)

event = return_event("EEFI-2021-06-23.TXT")
lambda_handler(event, None)

event = return_event("EESA-2021-06-23.TXT")
lambda_handler(event, None)
