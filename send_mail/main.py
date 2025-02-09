import functions_framework
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from python_http_client.exceptions import HTTPError


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    invoice_no=request_args['invoice_number']
    payment= request_args['payment_amount']
    agency=request_args['agency_acc']

    print("The variables are: ",invoice_no)

    html_content_with_vr = f'Hi,<br><br> An invoice with invoice number: {invoice_no} <br> with amount {payment} via agency {agency} is pending your approval. <br> Thank you.'

    message = Mail(
    from_email='aipr.gcp@gmail.com',
    to_emails='aditix2008@gmail.com',
    subject='Invoice Approval Request',
    html_content=html_content_with_vr)

    sg = SendGridAPIClient('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')

    try:
        response = sg.send(message)
        return f"email.status_code={response.status_code}"
        #expected 202 Accepted

    except HTTPError as e:
        return e.message





