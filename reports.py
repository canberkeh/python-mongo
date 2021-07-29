import os
import json
import pymongo

class main():

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["db"]
    counts = db["counts"]
    master = db["master"]

    read_counts = open('input_files/counts.json')
    read_master = open('input_files/master.json')

    counts_data = json.load(read_counts)
    master_data = json.load(read_master)

    counts_result = counts.insert_many(counts_data)
    master_result = master.insert_many(master_data)

    os.mkdir("output_files")

    location_barcode_amount_report = open("output_files/location_barcode_amount_report.txt","w+")
    barcode_amount_report = open("output_files/barcode_amount_report.txt","w+")
    aggregated_report = open("output_files/aggregated_report.txt","w+")
    location_barcode_amount_report.write("location;barcode;amount\n")
    barcode_amount_report.write("barcode;amount\n")
    aggregated_report.write("location;barcode;amount;sku;urun adi\n")

    for data in counts.find():
        for content in data["completedCounts"][0]["contents"]:
            barcode = content["barcode"]
            row = data["locationCode"] + ";" + str(barcode) + ";" + str(content["amount"])
            location_barcode_amount_report.write(row + "\n")
            barcode_amount_report.write(str(content["barcode"]) + ";" + str(content["amount"]) + "\n")
            product = master.find_one({"barcode": barcode})
            aggregated_report.write(row + ";" + product["sku"] + ";" + product["urun adi"] + "\n")


if __name__=='__main__':
    main()
