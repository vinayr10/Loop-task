install the all requirements.
run the main.py file.
make sure the csv files path are defined correctly according to your directory.
make sure the server starts running .
sent a post request 'http://127.0.0.1:8000/trigger_report' which triggers generation of the report.
sent a get request   'http://127.0.0.1:8000/get_report/{report_id}' which shows the status and if completed then shows the url of the file.
