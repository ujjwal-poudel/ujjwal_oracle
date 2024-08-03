from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import oracledb
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@csrf_exempt
def createTables(request):
    if request.method == 'POST':
        connection = None
        cursor = None
        try:
            # Parse the JSON body of the request
            data = json.loads(request.body)
            prod_id = data.get('prod_id')
            quantity = data.get('quantity')
            warehouse = data.get('warehouse')

            # Log the received data
            logger.info(f"Received data: prod_id={prod_id}, quantity={quantity}, warehouse={warehouse}")

            # Validate the parameters
            if prod_id is None or quantity is None or warehouse is None:
                error_msg = 'Missing parameters. Expected prod_id, quantity, and warehouse.'
                logger.error(error_msg)
                return JsonResponse({'error': error_msg}, status=400)

            # Convert prod_id and quantity to integers
            try:
                prod_id = int(prod_id)
                quantity = int(quantity)
            except ValueError as e:
                error_msg = f'prod_id and quantity must be integers. Error: {str(e)}'
                logger.error(error_msg)
                return JsonResponse({'error': error_msg}, status=400)

            connection = oracledb.connect(
                user='COMP214_M24_zo_107',
                password='password',
                dsn='199.212.26.208:1521/SQLD'
            )
            cursor = connection.cursor()

            # PL/SQL block to execute
            plsql_block = """
            BEGIN
                sp_update_inventory(:prod_id, :quantity, :warehouse);
            END;
            """

            # Execute the PL/SQL block with bind variables
            cursor.execute(plsql_block, {'prod_id': prod_id, 'quantity': quantity, 'warehouse': warehouse})
            connection.commit()

            logger.info("Stored procedure sp_update_inventory executed successfully.")
            return JsonResponse({'message': 'Stored procedure sp_update_inventory executed successfully.'})

        except json.JSONDecodeError as e:
            error_msg = f'Invalid JSON: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=400)
        except oracledb.DatabaseError as e:
            error_msg = f'Database error: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=500)
        except Exception as e:
            error_msg = f'Unexpected error: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=500)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        return JsonResponse({'error': 'Only POST method is allowed'}, status=405)

@csrf_exempt
def handle(request):
    if request.method == 'POST':
        connection = None
        cursor = None
        try:
            data = json.loads(request.body)
            prod_id = data.get('prod_id')
            
            logger.info(f"Received request for product ID: {prod_id}")

            if prod_id is None:
                error_msg = 'prod_id parameter is required'
                logger.error(error_msg)
                return JsonResponse({'error': error_msg}, status=400)
            
            try:
                prod_id = int(prod_id)
            except ValueError:
                error_msg = 'prod_id must be an integer'
                logger.error(error_msg)
                return JsonResponse({'error': error_msg}, status=400)

            connection = oracledb.connect(
                user='COMP214_M24_zo_107',
                password='password',
                dsn='199.212.26.208:1521/SQLD'
            )
            cursor = connection.cursor()

            # Enable DBMS_OUTPUT
            cursor.callproc('dbms_output.enable')

            # PL/SQL block to execute
            plsql_block = """
            DECLARE
                v_inventory_level NUMBER;
            BEGIN
                v_inventory_level := fn_get_product_inventory(:prod_id);
                dbms_output.put_line(
                    'Inventory : ' || v_inventory_level || ' for product id=' || :prod_id
                );
            END;
            """

            # Execute the PL/SQL block with bind variable
            cursor.execute(plsql_block, {'prod_id': prod_id})

            # Fetch output from DBMS_OUTPUT
            output = []
            while True:
                output_line = cursor.var(oracledb.STRING)
                cursor.callproc('dbms_output.get_line', [output_line, None])
                if output_line.getvalue() is None:
                    break
                output.append(output_line.getvalue())

            logger.info(f"Function executed successfully. Output: {output}")
            return JsonResponse({'output': output})

        except json.JSONDecodeError as e:
            error_msg = f'Invalid JSON: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=400)
        except oracledb.DatabaseError as e:
            error_msg = f'Database error: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=500)
        except Exception as e:
            error_msg = f'Unexpected error: {str(e)}'
            logger.error(error_msg)
            return JsonResponse({'error': error_msg}, status=500)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    else:
        return JsonResponse({'error': 'Only POST method is allowed'}, status=405)