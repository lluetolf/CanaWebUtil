import functions_framework
import os
import re
import email
from bs4 import BeautifulSoup
from google.cloud import storage
from google.cloud import firestore
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
PROJECT_ID = os.environ.get('GCP_PROJECT')
COLLECTION_NAME="Receivable"
firestore_client = firestore.Client(project=PROJECT_ID)
storage_client = storage.Client()

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def process_email(cloud_event):
    _print_info(cloud_event)

    bucket = storage_client.get_bucket(cloud_event.data["bucket"])
    blob_bytes = bucket.blob(cloud_event.data["name"]).download_as_string()

    email_message = email.message_from_bytes(blob_bytes)

    # Read subject
    subject = email_message.get("subject", "")
    harvest = re.findall(r"\d+/\d+", subject)[0]
    phase = subject.split(" ")[2].lower().replace("liquidacion", "liquidation").replace("preliquidacion", "preliquidation")

    # Read content
    if email_message.is_multipart():
        for part in email_message.walk():
            if "text/html" in part.get_content_type():
                message = part.get_payload(decode=True)
                soup = BeautifulSoup(message)
                title = soup.find_all("p")[0].getText()
                table = soup.find_all("table")[0]
                rows = table.find_all("tr")
                headers = list(map(lambda x: x.text.strip().replace(" ", "_").replace("%", "PPT"), rows[0].find_all('td')))
                values = list(map(lambda x: x.text.strip(), rows[1].find_all('td')))
                result = dict(zip(headers, values))
                result["title"] = title

                # Create firstore document
                entry = {}
                entry["ingenioId"] = result.get("CLAVE", "")
                entry["pricePerUnit"] = locale.atof(result.get("P.U", ""))
                entry["name"] = result.get("NOMBRE", "")
                entry["tons"] = locale.atof(result.get("TONS", ""))
                entry["total"] = locale.atof(result.get("TOTAL", ""))

                entry["raw"] = result

                harvest = harvest.replace('/', '-')
                key = f"{entry['ingenioId']}_{harvest}"
                a = {phase: entry}

                # write to a new doc in Firestore
                doc_ref = firestore_client.collection(COLLECTION_NAME).document(key)
                doc = doc_ref.get()
                if doc.exists:
                    d = doc.to_dict()
                    d[phase] = entry
                    doc_ref.set(d)
                else:
                    a["harvest"] = harvest
                    doc_ref.set(a)
    else:
        print("Did not work.")


def _print_info(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    bucket_name = data["bucket"]
    file_name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    print(f"ProjectId: {PROJECT_ID}")