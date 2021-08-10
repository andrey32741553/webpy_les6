import os
import sqlite3
import time
from email.message import EmailMessage
import aiosmtplib
from celery import Celery
from flask import Flask, jsonify
from celery.result import AsyncResult

app_name = "app"
app = Flask(app_name)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
connect = sqlite3.connect('contacts.db')
cursor = connect.cursor()
emails = [(first_name, email) for first_name, email in cursor.execute("SELECT first_name, email FROM contacts")
          .fetchall()]

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)


celery.conf.update(app.config)
celery.Task = ContextTask


@celery.task()
def send_messages(emails):
    for data in emails:
        adress = data[1]
        name = data[0]
        message = EmailMessage()
        message["From"] = os.getenv('MY_EMAIL')
        message["To"] = adress
        message["Subject"] = "Благодарность"
        message.set_content(f"Уважаемый {name}!\n"
                            "Спасибо, что пользуетесь нашим сервисом объявлений.")
        try:
            aiosmtplib.send(message,
                            hostname="smtp.mail.ru",
                            port=465, use_tls=True,
                            password=os.getenv('MY_PASSWORD'),
                            username=os.getenv('MY_EMAIL'))
        except:
            pass
        message = f"Message for {name} was send on {adress}"
        print(message)
    result = "Done"
    return result


@app.route('/send', methods=['POST'], strict_slashes=False)
def main():
    with app.app_context():
        task = send_messages.delay(emails)
        return jsonify({'task_id': task.id})


@app.route('/get status/<string:task_id>', methods=['GET'], strict_slashes=False)
def get_status(task_id):
    with app.app_context():
        task = AsyncResult(task_id, app=celery)
        return jsonify({'status': task.status,
                        'result': task.result
                        })


sender = main()
task_id = sender.json
status = get_status(task_id['task_id'])
info = status.json
print(info['status'], info['result'])
status = get_status(task_id['task_id'])
time.sleep(10)
print(info['status'], info['result'])

if __name__ == "__main__":
    app.run()
