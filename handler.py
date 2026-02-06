from main import main
import json

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    try:
        # Run the main analysis logic
        main()
        
        return {
            'statusCode': 200,
            'body': json.dumps('Stock Data Analysis completed successfully.')
        }
    except Exception as e:
        print(f"Error during execution: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
