# xml_to_csv

## Getting Started

1. Setup [MINIO](https://min.io/docs/minio/kubernetes/upstream/) in [Docker](https://www.docker.com/) container -
```
docker run -p 9000:9000 -it -p 9001:9001 -e "MINIO_ROOT_USER=minio99" -e "MINIO_ROOT_PASSWORD=minio123" quay.io/minio/minio server /data --console-address ":9001"
```
2. Clone the repo and install all the requirements.
```
git clone https://github.com/Pratyush-Saxena/xml_to_csv.git
cd xml_to_csv
pip3 install -r requirements.txt
```

3. Run the script -
```
python3 main.py
```

