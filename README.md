1.install the all requirements.

2.run the main.py file.

3.make sure the csv files path are defined correctly according to your directory.

4.make sure the server starts running .

5.sent a post request 'http://127.0.0.1:8000/trigger_report' which triggers generation of the report.

6.sent a get request   'http://127.0.0.1:8000/get_report/{report_id}' which shows the status and if completed then shows the url of the file.
