import sys
import json
from datetime import datetime, timedelta
import sqlite3
from PyQt5 import QtWidgets
from gui import Ui_MainWindow


def create_table():
    connection = sqlite3.connect("smqt.db")
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS smqt(uid, name, tm_type, energy_object, create_date, quality_code, 
    code_desc, value text, entry_point, author, duration, raw_date, raw_quality_code, raw_value text)''')
    connection.close()


def main():
    connection = sqlite3.connect("smqt.db")
    cursor = connection.cursor()

    with open("data.json", 'r', encoding="utf-8") as file:
        data = json.load(file)

    # with open("data.csv", "w") as file:
    #     writer = csv.writer(file)
    #
    #     writer.writerow(
    #         (
    #             "uid",
    #             "Наименование",
    #             "Вид телеметрии",
    #             "Энергообъект",
    #             "Дата создания",
    #             "Код качества",
    #             "Описание кода качества",
    #             "Значение измерения",
    #             "Узел телеметрии",
    #             "Автор",
    #             "Длительность",
    #             "Время сырого",
    #             "Код качества сырого",
    #             "сырое значение"
    #         )
    #     )

    # out_for_csv = []

    out_for_db = []
    for i in data["content"]:
        uid = i['telemetry']['objectIdSk11']

        name = i['telemetry']['name']

        tm_type = i['telemetry']['telemetryType']

        energy_object = i['telemetry']['energyObjectName']

        create_date = i['createdDate']
        create_date = datetime.fromisoformat(create_date.replace("Z", ''))
        create_date = create_date + timedelta(hours=3)
        create_date = create_date.strftime("%d.%m.%Y %H:%M:%S")

        quality_code = i['qualityCode']['code']
        quality_code = hex(quality_code)

        code_desc = i['qualityCode']['name']

        value = i['telemetryValue']

        entry_point = i['telemetry']['unitName']

        author = i['createdBy']

        duration = i['duration']

        raw_date = i['rawDate']
        raw_date = datetime.fromisoformat(raw_date.replace("Z", ''))
        raw_date = raw_date + timedelta(hours=3)
        raw_date = raw_date.strftime("%d.%m.%Y %H:%M:%S")

        raw_quality_code = i['rawQualityCode']
        raw_quality_code = hex(raw_quality_code)

        raw_value = i['rawValue']

        data_db = [uid, name, tm_type, energy_object, create_date, quality_code, code_desc, value, entry_point, author,
                   duration, raw_date, raw_quality_code, raw_value]

        # out_for_csv.append(
        #     {
        #         "uid": uid,
        #         "name": name,
        #         "tm_type": tm_type,
        #         "energy_object": energy_object,
        #         "create_date": create_date,
        #         "quality_code": quality_code,
        #         "code_desc": code_desc,
        #         "value": value,
        #         "entry_point": entry_point,
        #         "author": author,
        #         "duration": duration,
        #         "raw_date": raw_date,
        #         "raw_quality_code": raw_quality_code,
        #         "raw_value": raw_value
        #     }
        # )

        out_for_db.append(data_db)

        # with open("data.csv", "a") as file:
        #     writer = csv.writer(file)
        #
        #     writer.writerow(
        #         (
        #             uid,
        #             name,
        #             tm_type,
        #             energy_object,
        #             create_date,
        #             quality_code,
        #             code_desc,
        #             value,
        #             entry_point,
        #             author,
        #             duration,
        #             raw_date,
        #             raw_quality_code,
        #             raw_value
        #         )
        #     )

    cursor.execute('SELECT uid FROM smqt')
    if cursor.fetchone() is None:
        cursor.executemany('''INSERT INTO smqt VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', out_for_db)
        connection.commit()
    else:
        print('такая запись уже есть')
    connection.close()


def application():

    app = QtWidgets.QApplication(sys.argv)
    window = QtWidgets.QMainWindow()
    ui = Ui_MainWindow()
    ui.setupUi(window)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    create_table()
    main()
    application()
