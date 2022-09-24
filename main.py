import requests
import xml.etree.ElementTree as ET
import zipfile
import csv
import xml.dom.minidom
import boto3


# upload to miniocloud
def upload_to_s3(filename, object):
    host = "http://127.0.0.1:9000"
    access_key = "masoud"
    secret_key = "Strong#Pass#2022"
    session = boto3.resource(
        endpoint_url=host,
        config=boto3.session.Config(signature_version="s3v4"),
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # create bucket if not exists
    bucket_name = "masoud"
    if not session.Bucket(bucket_name) in session.buckets.all():
        session.create_bucket(Bucket=bucket_name)
    obj = session.Object(bucket_name, filename)
    obj.put(Body=object)


def prettify_xml(xmlfile):
    dom = xml.dom.minidom.parse(xmlfile)
    pretty_xml_as_string = dom.toprettyxml()
    return pretty_xml_as_string


def convert_to_csv(xmlfile):
    # Convert the contents of the xml into a CSV with the following header:
    # xmlfile=prettify_xml(xmlfile)
    tree = ET.parse(xmlfile)
    root = tree.getroot()
    documents = root[1][0][0][1:]
    with open("tmp.csv", "w") as f:
        header = [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]
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

    try:
        upload_to_s3(xmlfile.name.replace(".xml", ".csv"), open("tmp.csv", "rb"))
    except Exception as e:
        print("file upload "+xmlfile.name.replace(".xml", ".csv")+" failed", e)


def extract_data():
    # # Download the xml from this link
    url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"

    resp = requests.get(url)
    with open("files.xml", "wb") as f:
        f.write(resp.content)

    # From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
    tree = ET.parse("files.xml")
    root = tree.getroot()
    for doc in root.findall("result/doc"):
        zip_link = doc.find('str[@name="download_link"]').text
        resp = requests.get(zip_link)
        with open("tmp.zip", "wb") as f:
            f.write(resp.content)
        archive = zipfile.ZipFile("tmp.zip", "r")
        xmlfile = archive.open(archive.namelist()[0])
        try:
            convert_to_csv(xmlfile)
        except Exception as e:
            print("file convert "+xmlfile.name+" failed", e)


if __name__ == "__main__":
    extract_data()
