import requests
import xml.etree.ElementTree as ET
import zipfile
import csv
import xml.dom.minidom
import boto3
import os
from dotenv import load_dotenv
import logging
import tempfile
import io


class S3:
    def __init__(self):
        try:
            self.session = boto3.resource(
                endpoint_url=os.getenv("ENDPOINT_URL"),
                config=boto3.session.Config(signature_version="s3v4"),
                service_name="s3",
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            )
        except Exception as e:
            logging.error(e)
            logging.warning("Could not connect to S3")

    def upload_file(self, file_name, bucket, object):
        try:
            # create bucket if not exists
            if not self.session.Bucket(bucket) in self.session.buckets.all():
                self.session.create_bucket(Bucket=bucket)

            # upload file
            obj = self.session.Object(bucket, file_name)
            obj.upload_file(object)
            logging.info(f"File {file_name} uploaded to S3")
        except Exception as e:
            logging.error(e)


class XMLParser:
    def __init__(self):
        self.s3 = S3()

    def convert_to_csv_and_upload(self, xmlfile):
        try:
            logging.info("Converting file %s to CSV", xmlfile.name)
            tree = ET.parse(xmlfile)
            root = tree.getroot()
            documents = root[1][0][0][1:]
            tmpcsv = tempfile.NamedTemporaryFile(mode="w", delete=False)
            header = [
                "FinInstrmGnlAttrbts.Id",
                "FinInstrmGnlAttrbts.FullNm",
                "FinInstrmGnlAttrbts.ClssfctnTp",
                "FinInstrmGnlAttrbts.CmmdtyDerivInd",
                "FinInstrmGnlAttrbts.NtnlCcy",
                "Issr",
            ]
            with open(tmpcsv.name, "w") as f:
                writer = csv.DictWriter(f, fieldnames=header)
                writer.writeheader()
                for document in documents:
                    data = {}
                    for element in document:
                        rec = element[0]
                        data["FinInstrmGnlAttrbts.Id"] = rec[0].text
                        data["FinInstrmGnlAttrbts.FullNm"] = rec[1].text
                        data["FinInstrmGnlAttrbts.ClssfctnTp"] = rec[3].text
                        data["FinInstrmGnlAttrbts.CmmdtyDerivInd"] = rec[4].text
                        data["FinInstrmGnlAttrbts.NtnlCcy"] = rec[5].text
                        data["Issr"] = element[1].text
                    writer.writerow(data)
                self.s3.upload_file(
                    xmlfile.name.replace(".xml", ".csv"), "csv", tmpcsv.name
                )
        except Exception as e:
            logging.error(e.with_traceback())

    def prettify_xml(self, xmlfile):
        dom = xml.dom.minidom.parse(xmlfile)
        pretty_xml_as_string = dom.toprettyxml()
        return pretty_xml_as_string

    def extract(self, doc):
        try:
            zip_link = doc.find('str[@name="download_link"]').text
            resp = requests.get(zip_link)
            logging.info("Unzipping file %s", zip_link)
            archive = zipfile.ZipFile(io.BytesIO(resp.content), "r")
            xmlfile = archive.open(archive.namelist()[0])
            return xmlfile
        except Exception as e:
            logging.error(e.with_traceback())
            return None

    def parse(self,URL):
        try:
            resp = requests.get(URL)
            xmldata = resp.content
            root = ET.fromstring(xmldata)
            for doc in root.findall("result/doc"):
                xmlfile = self.extract(doc)
                if xmlfile:
                    self.convert_to_csv_and_upload(xmlfile)
                else:
                    logging.warning(
                        "Skipping file %s", doc.find('str[@name="download_link"]').text
                    )
        except Exception as e:
            logging.error(e.with_traceback())


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(
        filename="script.log",
        filemode="w",
        format="%(process)d-%(levelname)-8s %(asctime)s :: %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
    )
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Starting script")
    parser = XMLParser()
    logging.info("Parsing in progress..")
    parser.parse(os.getenv("URL"))
    logging.info("Parsing finished")
