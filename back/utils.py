import os
import types
import uuid

import numpy as np
import pandas as pd
from fastapi import Security, HTTPException, Depends, APIRouter
from fastapi.security import OAuth2PasswordBearer
from Api.paginationResultModel import PaginationResultModel
from Domain.Enums.therapyType import TherapyType
from Domain.Enums.сalciumType import CalciumType
from Domain.biobankItem import BiobankItem
from Domain.inspection import Inspection
from Domain.patient import Patient
from Domain.region import Region
from Domain.roles import roles
from Domain.sample import Sample
from Domain.task import Task
from Domain.user import User
from database import SessionLocal, SQLALCHEMY_DATABASE_URL
import smtplib
import ssl
import random
import string
from email.message import EmailMessage
import jwt
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import re
from yoyo import read_migrations
from yoyo import get_backend
from datetime import date
from sqlalchemy import desc
from fastapi import Request

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"

ALGORITHM = "HS256"

page_size = 50

sender_email = os.getenv("SENDER_EMAIL", "Skvortsov.Kirill@endocrincentr.ru")
sender_email_password = os.getenv("SENDER_EMAIL_PASSWORD", "Qw123456")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def send_email(title: str, text: str, to: str):
    print("To: " + to)
    print("Title: " + title)
    print("Text: " + text)
    smtp_server = "mx01.endocrincentr.ru"
    port = 587
    context = ssl.create_default_context()

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, sender_email_password)
        msg = EmailMessage()
        msg['Subject'] = title
        msg['From'] = sender_email
        msg['To'] = to
        msg.set_content(text)
        server.send_message(msg)
    except Exception as e:
        # Print any error messages to stdout
        print(e)
    finally:
        server.quit()


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_user_token(user: User):
    expire = datetime.utcnow() + timedelta(minutes=480)
    return jwt.encode({"userId": str(user.Id), "email": user.Email, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(db: Session = Depends(get_db),
                     token: str = Security(OAuth2PasswordBearer(tokenUrl="/api/identity/getTokenByLoginAndPassword"))):
    try:
        jwt_decode = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except:
        raise HTTPException(status_code=401)
    user = db.query(User).filter(User.Id == jwt_decode['userId']).first()
    if not user.IsActive:
        raise HTTPException(status_code=401)
    return user


def check_permission(user: User, permission):
    if not have_permission(user, permission):
        raise HTTPException(status_code=405)


def have_permission(user: User, permission):
    role = next(filter(lambda r: r.id == user.RoleId, roles))
    find_permission = filter(lambda p: p.value == permission.value, role.permissions)
    if len(list(find_permission)) == 0:
        return False
    return True

prefixes = []


def add_route(router_base: APIRouter, method: types.ModuleType):
    if method.router.prefix not in prefixes:
        router_base.include_router(method.router)
        prefixes.append(method.router.prefix)


def get_page(db: Session, entity, page: int):
    return db.query(entity).limit(page_size).offset(page * page_size)


def create_pagination_result(db, entity, dto_type, page, mapper_lambda, filter_lambda=lambda x: x):
    result_query = filter_lambda(db.query(entity))
    print("Query: ", str(result_query))
    return PaginationResultModel[dto_type](
        page=page,
        rows=list(map(mapper_lambda, result_query.limit(page_size).offset(page * page_size))),
        totalCount=result_query.count())


def validate_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)


def run_migration():
    backend = get_backend(SQLALCHEMY_DATABASE_URL)
    os.environ["PYTHONUTF8"] = "1"
    migrations = read_migrations('./migrations')
    print('migrations.len: ' + str(len(migrations)))
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))


def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def get_number_of_days_after_received(received_date):
    if received_date is None:
        return 0
    return (date.today() - received_date.date()).days


def get_first_letter_with_dot(name):
    if name is None or name == '':
        return ''
    return f'{name[0]}.'


def get_patient_full_name(patient):
    if patient.Patronymic is None or patient.Patronymic == '':
        return f'{patient.Surname} {get_first_letter_with_dot(patient.Name)}'
    return f'{patient.Surname} {get_first_letter_with_dot(patient.Name)} {get_first_letter_with_dot(patient.Patronymic)}'


def get_sample_for_task(samples: [Sample], task: Task):
    return next(filter(lambda s: s.Id == task.SampleId, samples))


def get_sample_for_biobank_item(samples: [Sample], biobankItem: BiobankItem):
    return next(filter(lambda s: s.Id == biobankItem.SampleId, samples))



def create_cascade_filters(filters):
    if len(filters) == 0:
        return lambda q: q

    if len(filters) == 1:
        return filters[0]

    def apply_filters(q, index):
        if index == len(filters) - 1:
            return filters[index](q)
        else:
            return filters[index](apply_filters(q, index + 1))

    return lambda q: apply_filters(q, 0)


class FluentFilter:
    filters: list
    sort_type: str
    sortField: str

    def __init__(self, sort_type, sort_field):
        self.sort_type = sort_type
        self.sort_field = sort_field
        self.filters = []

    def build(self, id_field_default_sort=None):
        if len(self.filters) == 0:
            return lambda q: q
        if id_field_default_sort is not None:
            return lambda q: create_cascade_filters(self.filters)(q).order_by(id_field_default_sort)
        else:
            return lambda q: create_cascade_filters(self.filters)(q)
        # return (lambda filters: lambda q: (reduce(lambda f, a: f(a), filters))(q))(self.filters)
        # for filter in self.filters:
        #     base_query = filter(base_query)
        # return base_query

    def use_filter(self, field_value, filter):
        condition = field_value is not None and field_value != '' and field_value != False
        if condition:
            self.filters.append(filter)
        return self


    def use_join(self, join):
        self.filters.append(join)
        return self


    def sort_core(self, field_name, asc, decs):
        if field_name == self.sort_field:
            if self.sort_type == 'asc':
                self.filters.append(asc)
            if self.sort_type == 'desc':
                self.filters.append(decs)

        return self


    def use_sort(self, field_name, field):
        asc = lambda q: q.order_by(field)
        decs = lambda q: q.order_by(desc(field))
        self.sort_core(field_name, asc, decs)
        return self

    def use_sort_reverse(self, field_name, field):
        asc = lambda q: q.order_by(desc(field))
        decs = lambda q: q.order_by(field)
        self.sort_core(field_name, asc, decs)
        return self


def getDatePlusDays(date, days=1):
    return datetime.strptime(date, '%Y-%m-%d') + timedelta(days=days)


def update_insert_inspections_from_data_frame(db, table_df):
    for a in list(table_df.columns):
        print(a)
    table_df = table_df[table_df['Код пациента'].notnull()]
    table_df = table_df[table_df['Дата обследования'].notnull()]
    table_df['Полис ОМС'] = table_df['Полис ОМС'].apply(lambda x: '' if pd.isna(x) else str(int(x)))
    table_df['Полис ОМС'] = table_df['Полис ОМС'].astype("string")
    main_sudrgeon_arr = ['Лимфодиссекция', 'Гемитиреоидэктомия', 'Тиреоидэктомия', 'Другое', 'Нет данных',
                                           'Паратиреоидэктомия (ПТЭ)', 'Сочетанная операция (тиреоид/гемитиреоидэктомия с ПТЭ)']

    if 'Другой Вид и хар. хир. вмеш-ва' not in table_df.columns:
        table_df['Другой Вид и хар. хир. вмеш-ва'] = table_df['Вид и хар. хир. вмеш-ва'].\
            apply(lambda x: x if x not in main_sudrgeon_arr else '')
        table_df['Вид и хар. хир. вмеш-ва'] = table_df['Вид и хар. хир. вмеш-ва'].\
            apply(lambda x: x if x in main_sudrgeon_arr else '')

    if 'Другой Вид и хар. повтор. хир. вмеш-ва' not in table_df.columns:
        table_df['Другой Вид и хар. повтор. хир. вмеш-ва'] = table_df['Вид и хар. повтор. хир. вмеш-ва'].\
            apply(lambda x: x if x not in main_sudrgeon_arr else '')
        table_df['Вид и хар. повтор. хир. вмеш-ва'] = table_df['Вид и хар. повтор. хир. вмеш-ва'].\
            apply(lambda x: x if x in main_sudrgeon_arr else '')

    table_df['Кальц.ГМ'] = table_df['Кальц.ГМ'].apply(lambda x: '' if x == 'Нет данных' else x)
    table_df['Кальц.ГМ, другое'] = table_df['Кальц.ГМ'].apply(lambda x: '' if str(x).lower() in ['есть', 'нет'] else x)

    table_df['Кальц.ГМ'] = table_df['Кальц.ГМ'].apply(lambda x: '' if str(x).lower() not in ['есть', 'нет'] else x)

    table_df['Сут.доза Альфакальцидол'] = table_df['Сут.доза Альфакальцидол'].apply(lambda x: x if str(x).isnumeric() else 0)
    table_df['Сут.доза Кальцитриол'] = table_df['Сут.доза Кальцитриол'].apply(lambda x: re.sub(r"[^0-9,.]", '', str(x)))
    table_df['Сут.доза Карбонат кальция'] = table_df['Сут.доза Карбонат кальция'].apply(lambda x: 0)
    table_df['Сут.доза Гидрохлортиазид'] = table_df['Сут.доза Гидрохлортиазид'].apply(lambda x: 0)
    table_df['Сут.доза Рекомбинантный ПТГ'] = table_df['Сут.доза Рекомбинантный ПТГ'].apply(lambda x: 0)

    table_df['Инвалидность'] = table_df['Инвалидность'].apply(lambda x: 'нет инвалидности' if str(x).lower() == 'нет' else x)
    table_df['Инвалидность'] = table_df['Инвалидность'].apply(lambda x: 'ребенок-инвалид' if str(x).lower() == 'ребенок инвалид' else x)
    table_df['Микролитиаз'] = table_df['Микролитиаз'].apply(lambda x: 'Отсутствует' if str(x).lower() == 'нет' else x)
    table_df['Нефролитиаз'] = table_df['Нефролитиаз'].apply(lambda x: 'Отсутствует' if str(x).lower() == 'нет' else x)
    table_df['Нефрокальциноз'] = table_df['Нефрокальциноз'].apply(lambda x: 'Отсутствует' if str(x).lower() == 'нет' else x)
    table_df['СМП для купир. остр. гипоСа'] = table_df['СМП для купир. остр. гипоСа'].apply(lambda x: '0' if str(x).lower() == 'нет' else x)
    table_df['СМП для купир. остр. гипоСа'] = table_df['СМП для купир. остр. гипоСа'].apply(lambda x: '0' if str(x).lower() == '.' else x)
    table_df['Наличие сибсов'] = table_df['Наличие сибсов'].apply(lambda x: '0' if str(x).lower() == 'нет' else x)
    table_df['Альбумин-скорректированный кальций'] = table_df['Альбумин-скорректированный кальций'].apply(lambda x: None if str(x) == '0' else x)
    table_df['ТЭ/гемиТЭ, предопер. пат.'] = table_df['ТЭ/гемиТЭ, предопер. пат.'].apply(lambda x: None if x == 'нет данных' else x)
    table_df['ТЭ/гемиТЭ, объем хир.вмеш-ва'] = table_df['ТЭ/гемиТЭ, объем хир.вмеш-ва'].apply(lambda x: None if x == 'нет данных' else x)

    table_df['Уровень ПТГ, пг/мл или пмоль/л'] = table_df['Уровень ПТГ, пг/мл или пмоль/л'].apply(lambda x: str(x).replace(' ', ''))
    table_df['Длит.заб., лет'] = table_df['Длит.заб., лет'].apply(lambda x: str(x).split(' ')[0])


    regions = db.query(Region).all()

    def get_region(x):
        candidates = list(filter(lambda region: str(x) in region.Aliases or str(x) == region.Name, regions))
        if len(candidates) == 0:
            return ''
        return candidates[0].Name

    table_df['Регион'] = table_df['Регион'].apply(get_region)

    def get_first_phrase(string):
        string = string.replace("\n", " ")
        for i, char in enumerate(string):
            if char.isupper() and string[i - 1] == " ":
                return string[:i]
        return string

    table_df["Соц. статус"] = table_df["Соц. статус"].apply(str)
    table_df['Соц. статус'] = table_df['Соц. статус'].apply(get_first_phrase)

    if 'Единица измерения ПТГ' not in table_df.columns:
        table_df['Единица измерения ПТГ'] = table_df['Код пациента'].apply(lambda x: '')

    added_patients = 0
    added_inspections = 0
    total_processed_inspections = 0

    for i, row in table_df.iterrows():
        patient = db.query(Patient).filter(Patient.QmsId == row['Код пациента']).first()
        if patient is None:
            patient = Patient()
            patient.QmsId = row['Код пациента']

        patient.Name = row['Имя']
        patient.Surname = row['Фамилия']
        patient.Patronymic = row['Отчество']
        patient.Gender = 0 if row['Пол'] == 'Мужской' else 1
        patient.Dob = row['Дата рождения']

        patient.Snils = row['СНИЛС']
        patient.Policy = row['Полис ОМС']
        patient.Document = row['Документ']
        patient.Invalidity = row['Инвалидность']
        patient.SocialStatus = row['Соц. статус']
        patient.Address = row['Адрес']
        patient.Region = row['Регион']
        patient.City = row['Район/округ/город']
        patient.TelephoneNumber = row['Телефон']
        patient.Email = row['Эл. почта']
        patient.Race = row['Раса']

        if patient.Id is None:
            patient.Id = uuid.uuid4()
            db.add(patient)
            added_patients += 1

        inspection = db.query(Inspection)\
            .filter(Inspection.PatientId == patient.Id)\
            .filter(Inspection.Date == row['Дата обследования'])\
            .first()

        if inspection is None:
            inspection = Inspection()
            inspection.PatientId = patient.Id
            inspection.Date = row['Дата обследования']

        def get_value(val, field_type):
            if field_type == 'bool' and type(val) is str and (val.lower() == 'да' or val.lower() == 'есть'):
                return True
            if field_type == 'bool' and type(val) is bool and val:
                return True
            if field_type == 'bool' and type(val) is str and (val.lower() == 'нет' or val.lower() == ''):
                return False
            if field_type == 'bool':
                return False
            if field_type == 'datetime' and str(val) == 'NaT':
                return None
            if field_type == 'float' and type(val) is str:
                val = val.replace('>', '')
                val = val.replace(' ', '')
                val = val.replace(',', '.')
                if val == 'нет' or val == '.' or val.startswith('=') or val == '' or val == 'None' or val == 'nan':
                    val = np.nan
            return None if val is None or str(val).lower() == 'nan' or str(val) == 'нет данных' or str(val) == 'Нет данных' else str(val)

        inspection.DiseaseStatus = get_value(row['Статус забол.на момент обследов.'], 'str')
        inspection.LpuAttachments = get_value(row['ЛПУ прикрепления'], 'str')
        inspection.LechsFullNameDoctor = get_value(row['ФИО леч. врача'], 'str')
        inspection.EncMedicalCardNumber = get_value(row['Номер амб. карты ЭНЦ'], 'str')
        inspection.TheNumberOfTheMedicalCentersMedicalCard = get_value(row['Номер амб. карты ЛПУ'], 'str')
        inspection.Diagnosis = get_value(row['Диагноз'], 'str')
        inspection.DateOfOnsetOfTheDisease = get_value(row['Дата начала заболевания'], 'datetime')
        inspection.TheDateIsSetDiagnosis = get_value(row['Дата устан. диагноза'], 'datetime')
        inspection.TheLevelOfPthPgmlOrPmoll = get_value(row['Уровень ПТГ, пг/мл или пмоль/л'], 'str')
        inspection.AgeOfMouthdiagnosisYears = get_value(row['Возраст уст.диагн., лет'], 'float')
        inspection.LastszabYears = get_value(row['Длит.заб., лет'], 'str')
        inspection.Etiolzab = get_value(row['Этиол.заб.'], 'str')
        inspection.EtiolzabOther = get_value(row['Этиол.заб. Другое'], 'str')
        inspection.ZabForm = get_value(row['Форма заб.'], 'str')
        inspection.Isolated = get_value(row['Изолированный'], 'str')
        inspection.InCompGenetSyndrome = get_value(row['В сост. генет. синдрома'], 'str')
        inspection.DateOfOperation = get_value(row['Дата операции'], 'datetime')
        inspection.TypeAndHarHirIntervention = get_value(row['Вид и хар. хир. вмеш-ва'], 'str')
        inspection.OtherTypeAndHarHirIntervention = get_value(row['Другой Вид и хар. хир. вмеш-ва'], 'str')
        inspection.TegemiteTheVolumeOfHirintervention = get_value(row['ТЭ/гемиТЭ, объем хир.вмеш-ва'], 'str')
        inspection.TegemitePrepayPat = get_value(row['ТЭ/гемиТЭ, предопер. пат.'], 'str')
        inspection.PteVolumeOfHirIntervention = get_value(row['ПТЭ, объем хир. вмеш-ва'], 'str')
        inspection.PtePrepayPat = get_value(row['ПТЭ, предопер. пат.'], 'str')
        inspection.RepeatHirIntervention = get_value(row['Повтор. хир. вмеш-во'], 'bool')
        inspection.DateOfRepeatHirIntervention = get_value(row['Дата повтор. хир. вмеш-ва'], 'datetime')
        inspection.ViewAndHarRepeatHirIntervention = get_value(row['Вид и хар. повтор. хир. вмеш-ва'], 'str')
        inspection.OtherViewAndHarRepeatHirIntervention = get_value(row['Другой Вид и хар. повтор. хир. вмеш-ва'], 'str')
        inspection.FatherTheVicesAreDevelopedInternalOrg = get_value(row['Отец, пороки разв. внутр. орг.'], 'bool')
        inspection.FatherHypopara = get_value(row['Отец, гипопара'], 'bool')
        inspection.FatherPrimaryNpochNedost = get_value(row['Отец, первич. н/поч. недост.'], 'bool')
        inspection.FatherEpilepsy = get_value(row['Отец, эпилепсия'], 'bool')
        inspection.FatherIcd = get_value(row['Отец, МКБ'], 'bool')
        inspection.FatherNephrocalcus = get_value(row['Отец, нефрокальц.'], 'bool')
        inspection.FatherStunted = get_value(row['Отец, низкоросл.'], 'bool')
        inspection.FatherHearingLoss = get_value(row['Отец, тугоухость'], 'bool')
        inspection.FatherUmstvFellBehind = get_value(row['Отец, умств. отстал.'], 'bool')
        inspection.FatherNoDataAvailable = get_value(row['Отец, нет данных'], 'bool')
        inspection.FatherOther = get_value(row['Отец, другое'], 'bool')
        inspection.MotherTheVicesAreDevelopedInternalOrg = get_value(row['Мать, пороки разв. внутр. орг.'], 'bool')
        inspection.MotherHypopara = get_value(row['Мать, гипопара'], 'bool')
        inspection.MotherPrimaryNpochNedost = get_value(row['Мать, первич. н/поч. недост.'], 'bool')
        inspection.MotherEpilepsy = get_value(row['Мать, эпилепсия'], 'bool')
        inspection.MotherIcd = get_value(row['Мать, МКБ'], 'bool')
        inspection.MotherNephrocalcus = get_value(row['Мать, нефрокальц.'], 'bool')
        inspection.MotherShort = get_value(row['Мать, низкоросл.'], 'bool')
        inspection.MotherHearingLoss = get_value(row['Мать, тугоухость'], 'bool')
        inspection.MotherUmstvFellBehind = get_value(row['Мать, умств. отстал.'], 'bool')
        inspection.MotherNoDataAvailable = get_value(row['Мать, нет данных'], 'bool')
        inspection.MotherOther = get_value(row['Мать, другое'], 'bool')
        inspection.ThePresenceOfSibs = get_value(row['Наличие сибсов'], 'str')
        inspection.Sibs1VicesOfDevelopmentInternalOrg = get_value(row['Сибс1, пороки разв. внутр. орг.'], 'bool')
        inspection.Sibs1Hypopara = get_value(row['Сибс1, гипопара'], 'bool')
        inspection.Sibs1PrimaryNpochNedost = get_value(row['Сибс1, первич. н/поч. недост.'], 'bool')
        inspection.Sibs1Epilepsy = get_value(row['Сибс1, эпилепсия'], 'bool')
        inspection.Sibs1Icd = get_value(row['Сибс1, МКБ'], 'bool')
        inspection.Sibs1Nephrocalts = get_value(row['Сибс1, нефрокальц.'], 'bool')
        inspection.Sibs1Undersized = get_value(row['Сибс1, низкоросл.'], 'bool')
        inspection.Sibs1HearingLoss = get_value(row['Сибс1, тугоухость'], 'bool')
        inspection.Sibs1UmstvFellBehind = get_value(row['Сибс1, умств. отстал.'], 'bool')
        inspection.Sibs1NoDataAvailable = get_value(row['Сибс1, нет данных'], 'bool')
        inspection.Sibs1No = get_value(row['Сибс1, нет'], 'bool')
        inspection.Sibs2VicesOfDevelopmentInternalOrg = get_value(row['Сибс2, пороки разв. внутр. орг.'], 'bool')
        inspection.Sibs2Hypopara = get_value(row['Сибс2, гипопара'], 'bool')
        inspection.Sibs2PrimaryNpochNedost = get_value(row['Сибс2, первич. н/поч. недост.'], 'bool')
        inspection.Sibs2Epilepsy = get_value(row['Сибс2, эпилепсия'], 'bool')
        inspection.Sibs2Icd = get_value(row['Сибс2, МКБ'], 'bool')
        inspection.Sibs2Nephrocalts = get_value(row['Сибс2, нефрокальц.'], 'bool')
        inspection.Sibs2Undersized = get_value(row['Сибс2, низкоросл.'], 'bool')
        inspection.Sibs2HearingLoss = get_value(row['Сибс2, тугоухость'], 'bool')
        inspection.Sibs2UmstvFellBehind = get_value(row['Сибс2, умств. отстал.'], 'bool')
        inspection.Sibs2NoDataAvailable = get_value(row['Сибс2, нет данных'], 'bool')
        inspection.Sibs2No = get_value(row['Сибс2, нет'], 'bool')
        inspection.Sibs3VicesOfDevelopmentInternalOrg = get_value(row['Сибс3, пороки разв. внутр. орг.'], 'bool')
        inspection.Sibs3Hypopara = get_value(row['Сибс3, гипопара'], 'bool')
        inspection.Sibs3PrimaryNpochNedost = get_value(row['Сибс3, первич. н/поч. недост.'], 'bool')
        inspection.Sibs3Epilepsy = get_value(row['Сибс3, эпилепсия'], 'bool')
        inspection.Sibs3Icd = get_value(row['Сибс3, МКБ'], 'bool')
        inspection.Sibs3Nephrocalts = get_value(row['Сибс3, нефрокальц.'], 'bool')
        inspection.Sibs3Undersized = get_value(row['Сибс3, низкоросл.'], 'bool')
        inspection.Sibs3HearingLoss = get_value(row['Сибс3, тугоухость'], 'bool')
        inspection.Sibs3UmstvFellBehind = get_value(row['Сибс3, умств. отстал.'], 'bool')
        inspection.Sibs3NoDataAvailable = get_value(row['Сибс3, нет данных'], 'bool')
        inspection.Sibs3No = get_value(row['Сибс3, нет'], 'bool')
        inspection.Sibs4VicesOfDevelopmentInternalOrg = get_value(row['Сибс4, пороки разв. внутр. орг.'], 'bool')
        inspection.Sibs4Hypopara = get_value(row['Сибс4, гипопара'], 'bool')
        inspection.Sibs4PrimaryNpochNedost = get_value(row['Сибс4, первич. н/поч. недост.'], 'bool')
        inspection.Sibs4Epilepsy = get_value(row['Сибс4, эпилепсия'], 'bool')
        inspection.Sibs4Icd = get_value(row['Сибс4, МКБ'], 'bool')
        inspection.Sibs4Nephrocalts = get_value(row['Сибс4, нефрокальц.'], 'bool')
        inspection.Sibs4Undersized = get_value(row['Сибс4, низкоросл.'], 'bool')
        inspection.Sibs4HearingLoss = get_value(row['Сибс4, тугоухость'], 'bool')
        inspection.Sibs4UmstvFellBehind = get_value(row['Сибс4, умств. отстал.'], 'bool')
        inspection.Sibs4NoDataAvailable = get_value(row['Сибс4, нет данных'], 'bool')
        inspection.Sibs4No = get_value(row['Сибс4, нет'], 'bool')
        inspection.AimFatherTheVicesAreDevelopedInternalOrg = get_value(row['А/им. Отец, пороки разв. внутр. орг.'],
                                                                        'bool')
        inspection.AimFatherHypopara = get_value(row['А/им. Отец, гипопара'], 'bool')
        inspection.AimFatherPrimaryNpochNedost = get_value(row['А/им. Отец, первич. н/поч. недост.'], 'bool')
        inspection.AimFatherEpilepsy = get_value(row['А/им. Отец, эпилепсия'], 'bool')
        inspection.AimFatherIcd = get_value(row['А/им. Отец, МКБ'], 'bool')
        inspection.AimFatherNephrocalts = get_value(row['А/им. Отец, нефрокальц.'], 'bool')
        inspection.AimFatherUndersized = get_value(row['А/им. Отец, низкоросл.'], 'bool')
        inspection.AimFatherHearing = get_value(row['А/им. Отец, тугоухость'], 'bool')
        inspection.LossAimFatherUmstvFellBehind = get_value(row['А/им. Отец, умств. отстал.'], 'bool')
        inspection.AimFatherThereIsNo = get_value(row['А/им. Отец, нет данных'], 'bool')
        inspection.AimDataFatherNo = get_value(row['А/им. Отец, нет'], 'bool')
        inspection.AimMotherTheVicesAreDevelopedInternalOrg = get_value(row['А/им. Мать, пороки разв. внутр. орг.'],
                                                                        'bool')
        inspection.AimMotherHypopara = get_value(row['А/им. Мать, гипопара'], 'bool')
        inspection.AimMotherPrimaryNpochNedost = get_value(row['А/им. Мать, первич. н/поч. недост.'], 'bool')
        inspection.AimMotherEpilepsy = get_value(row['А/им. Мать, эпилепсия'], 'bool')
        inspection.AimMotherIcd = get_value(row['А/им. Мать, МКБ'], 'bool')
        inspection.AimMotherNephrocalc = get_value(row['А/им. Мать, нефрокальц.'], 'bool')
        inspection.AimMotherUndersized = get_value(row['А/им. Мать, низкоросл.'], 'bool')
        inspection.AimMotherHearing = get_value(row['А/им. Мать, тугоухость'], 'bool')
        inspection.LossAimMotherUmstvFellBehind = get_value(row['А/им. Мать, умств. отстал.'], 'bool')
        inspection.AimMotherThereIsNo = get_value(row['А/им. Мать, нет данных'], 'bool')
        inspection.AimDataMotherNo = get_value(row['А/им. Мать, нет'], 'bool')
        inspection.ThePresenceOfSibs1 = get_value(row['Наличие сибсов.1'], 'str')
        inspection.AImSibs1VicesOfDevelopmentInternalOrg = get_value(row['А/им. Сибс1, пороки разв. внутр. орг.'],
                                                                     'bool')
        inspection.AimSibs1Hypopara = get_value(row['А/им. Сибс1, гипопара'], 'bool')
        inspection.AimSibs1PrimaryNpochNedost = get_value(row['А/им. Сибс1, первич. н/поч. недост.'], 'bool')
        inspection.AimSibs1Epilepsy = get_value(row['А/им. Сибс1, эпилепсия'], 'bool')
        inspection.AimSibs1Icd = get_value(row['А/им. Сибс1, МКБ'], 'bool')
        inspection.AimSibs1Nephrocalts = get_value(row['А/им. Сибс1, нефрокальц.'], 'bool')
        inspection.AimSibs1Undersized = get_value(row['А/им. Сибс1, низкоросл.'], 'bool')
        inspection.AimSibs1Hearing = get_value(row['А/им. Сибс1, тугоухость'], 'bool')
        inspection.LossAimSibs1UmstvFellBehind = get_value(row['А/им. Сибс1, умств. отстал.'], 'bool')
        inspection.AimSibs1ThereIsNo = get_value(row['А/им. Сибс1, нет данных'], 'bool')
        inspection.AimDataSibs1No = get_value(row['А/им. Сибс1, нет'], 'bool')
        inspection.AimSibs2VicesOfDevelopmentInternalOrg = get_value(row['А/им. Сибс2, пороки разв. внутр. орг.'],
                                                                     'bool')
        inspection.AimSibs2Hypopara = get_value(row['А/им. Сибс2, гипопара'], 'bool')
        inspection.AimSibs2PrimaryNpochNedost = get_value(row['А/им. Сибс2, первич. н/поч. недост.'], 'bool')
        inspection.AimSibs2Epilepsy = get_value(row['А/им. Сибс2, эпилепсия'], 'bool')
        inspection.AimSibs2Icd = get_value(row['А/им. Сибс2, МКБ'], 'bool')
        inspection.AimSibs2Nephrocalts = get_value(row['А/им. Сибс2, нефрокальц.'], 'bool')
        inspection.AimSibs2Undersized = get_value(row['А/им. Сибс2, низкоросл.'], 'bool')
        inspection.AimSibs2Hearing = get_value(row['А/им. Сибс2, тугоухость'], 'bool')
        inspection.LossAimSibs2UmstvFellBehind = get_value(row['А/им. Сибс2, умств. отстал.'], 'bool')
        inspection.AimSibs2ThereIsNo = get_value(row['А/им. Сибс2, нет данных'], 'bool')
        inspection.AimDataSibs2No = get_value(row['А/им. Сибс2, нет'], 'bool')
        inspection.AimSibs3VicesOfDevelopmentInternalOrg = get_value(row['А/им. Сибс3, пороки разв. внутр. орг.'],
                                                                     'bool')
        inspection.AimSibs3Hypopara = get_value(row['А/им. Сибс3, гипопара'], 'bool')
        inspection.AimSibs3PrimaryNpochNedost = get_value(row['А/им. Сибс3, первич. н/поч. недост.'], 'bool')
        inspection.AimSibs3Epilepsy = get_value(row['А/им. Сибс3, эпилепсия'], 'bool')
        inspection.AimSibs3Icd = get_value(row['А/им. Сибс3, МКБ'], 'bool')
        inspection.AimSibs3Nephrocalts = get_value(row['А/им. Сибс3, нефрокальц.'], 'bool')
        inspection.AimSibs3Undersized = get_value(row['А/им. Сибс3, низкоросл.'], 'bool')
        inspection.AimSibs3Hearing = get_value(row['А/им. Сибс3, тугоухость'], 'bool')
        inspection.LossAimSibs3UmstvFellBehind = get_value(row['А/им. Сибс3, умств. отстал.'], 'bool')
        inspection.AimSibs3ThereIsNo = get_value(row['А/им. Сибс3, нет данных'], 'bool')
        inspection.AimDataSibs3No = get_value(row['А/им. Сибс3, нет'], 'bool')
        inspection.AimSibs4VicesOfDevelopmentInternalOrg = get_value(row['А/им. Сибс4, пороки разв. внутр. орг.'],
                                                                     'bool')
        inspection.AimSibs4Hypopara = get_value(row['А/им. Сибс4, гипопара'], 'bool')
        inspection.AimSibs4PrimaryNpochNedost = get_value(row['А/им. Сибс4, первич. н/поч. недост.'], 'bool')
        inspection.AimSibs4Epilepsy = get_value(row['А/им. Сибс4, эпилепсия'], 'bool')
        inspection.AimSibs4Icd = get_value(row['А/им. Сибс4, МКБ'], 'bool')
        inspection.AimSibs4Nephrocalts = get_value(row['А/им. Сибс4, нефрокальц.'], 'bool')
        inspection.AimSibs4Undersized = get_value(row['А/им. Сибс4, низкоросл.'], 'bool')
        inspection.AimSibs4Hearing = get_value(row['А/им. Сибс4, тугоухость'], 'bool')
        inspection.LossAimSibs4UmstvFellBehind = get_value(row['А/им. Сибс4, умств. отстал.'], 'bool')
        inspection.AimSibs4ThereIsNo = get_value(row['А/им. Сибс4, нет данных'], 'bool')
        inspection.AimDataSibs4ThereIsNo = get_value(row['А/им. Сибс4, нет'], 'bool')
        inspection.MutationInTheAireGene = get_value(row['Мут. в гене AIRE'], 'str')
        inspection.AboveautoatIfnOmega = get_value(row['Превыш.аутоАТ ИФН омега'], 'bool')
        inspection.PrimaryNpochechNotEnough = get_value(row['Первич. н/почеч. недост.'], 'bool')
        inspection.SlizSkincandidiasis = get_value(row['Слиз.-кожн.кандидоз'], 'bool')
        inspection.DrcompsyndrHypothyroidismchait = get_value(row['Др.комп.синдр. Гипотиреоз/ХАИТ'], 'bool')
        inspection.DrcompsyndrSd1 = get_value(row['Др.комп.синдр. СД1'], 'bool')
        inspection.DrcompsyndrMalabsorb = get_value(row['Др.комп.синдр. Мальабсорб.'], 'bool')
        inspection.DrcompsyndrHypolasiaOfTheToothem = get_value(row['Др.комп.синдр. Гиполазия зуб.эм.'], 'bool')
        inspection.DrCompsyndrEnteropathy = get_value(row['Др.комп.синдр. Энтеропатии'], 'bool')
        inspection.DrcompsyndrHepatitis = get_value(row['Др.комп.синдр. Гепатит'], 'bool')
        inspection.DrcompsyndrJade = get_value(row['Др.комп.синдр. Нефрит'], 'bool')
        inspection.DrcompsyndrHypogonadism = get_value(row['Др.комп.синдр. Гипогонадизм'], 'bool')
        inspection.DrcompsyndrAlopecia = get_value(row['Др.комп.синдр. Алопеция'], 'bool')
        inspection.DrcompsyndrPygmret = get_value(row['Др.комп.синдр. Пигм.рет.'], 'bool')
        inspection.DrcompsyndrDefeatCnsOrPns = get_value(row['Др.комп.синдр. Пораж. ЦНС или ПНС'], 'bool')
        inspection.DrcompsyndrHearing = get_value(row['Др.комп.синдр. Тугоухость'], 'bool')
        inspection.LossDrcompsyndrNoDataAvailable = get_value(row['Др.комп.синдр. Нет данных'], 'bool')
        inspection.TheDate = get_value(row['Дата обследования'], 'datetime')
        inspection.OfTheExaminationLastssinceTheLastVisitYearsmonth = get_value(row['Длит.с посл. визита, лет/мес'],
                                                                                'str')
        inspection.ComplaintsSeizures = get_value(row['Жалобы, судороги'], 'bool')
        inspection.ComplaintsParesthesia = get_value(row['Жалобы, парестезии'], 'bool')
        inspection.ComplaintsBrainFog = get_value(row['Жалобы, мозг. туман'], 'bool')
        inspection.ComplaintsPainInTheBonesAndSust = get_value(row['Жалобы, боли в кост. и суст.'], 'bool')
        inspection.ComplaintsWeakness = get_value(row['Жалобы, слабость'], 'bool')
        inspection.ComplaintsOtherComplaints = get_value(row['Жалобы, др. жалобы'], 'bool')
        inspection.OtherComplaints = get_value(row['Другие жалобы'], 'str')
        inspection.HeightCm = get_value(row['Рост, см'], 'float')
        inspection.WeightKg = get_value(row['Вес, кг'], 'float')
        inspection.BmiKgm2 = get_value(row['ИМТ, кг/м2'], 'float')
        inspection.Menopause = get_value(row['Менопауза'], 'bool')
        inspection.RegularMenstrCycle = get_value(row['Регуляр. менстр. цикла'], 'str')
        inspection.BeremenOnTechMom = get_value(row['Беремен. на тек. мом.'], 'bool')
        inspection.TheOutcomeIsInevitable = get_value(row['Исход беремен.'], 'str')
        inspection.TrimWeTakeIt = get_value(row['Трим. берем.'], 'str')
        inspection.ComplicationsWeTakeIt = get_value(row['Ослож. берем.'], 'str')
        inspection.NumberOfBirths = get_value(row['Кол-во родов'], 'float')
        inspection.WeTakeTheNumber = get_value(row['Кол-во берем.'], 'float')
        inspection.DateOfLabissl = get_value(row['Дата лаб.иссл.'], 'str')
        inspection.CalciumTotal = get_value(row['Кальций общ.'], 'float')
        inspection.Albumin = get_value(row['Альбумин'], 'float')
        albuminadjustedCalcium = get_value(row['Альбумин-скорректированный кальций'], 'float')
        inspection.AlbuminadjustedCalcium = None if albuminadjustedCalcium == 0 else albuminadjustedCalcium
        inspection.Phosphorus = get_value(row['Фосфор'], 'float')
        inspection.CalciumIon = get_value(row['Кальций ион.'], 'float')
        inspection.Magnesium = get_value(row['Магний'], 'float')
        inspection.Schf = get_value(row['ЩФ'], 'float')
        inspection.Creatinine = get_value(row['Креатинин'], 'float')
        inspection.GfrByEpi = get_value(row['СКФ по EPI'], 'float')
        inspection.SkfByFormschwartz = get_value(row['СКФ по форм.Шварца'], 'float')
        inspection.d25OhD = get_value(row['25(OH)D'], 'float')
        inspection.Calciurday = get_value(row['Кальциур.сут.'], 'float')
        inspection.Kfk = get_value(row['КФК'], 'float')
        inspection.Sth = get_value(row['СТХ'], 'float')
        inspection.Ok = get_value(row['ОК'], 'float')
        inspection.Calciurutr = get_value(row['Кальциур.утр.'], 'float')
        inspection.Calccreatineutrurine = get_value(row['Кальц./креат.утр.моча'], 'float')
        inspection.Calccreatdayurine = get_value(row['Кальц./креат.сут.моча'], 'str')
        inspection.SmpForKupirSharpHyposa = get_value(row['СМП для купир. остр. гипоСа'], 'str')
        inspection.HospitalAboutTheAcuteHyposa = get_value(row['Госпитал. по поводу остр. гипоСа'], 'str')
        inspection.PatkidneyDateOfService = get_value(row['Пат.почек, дата обсл.'], 'datetime')
        inspection.PatkidneyMetvisa = get_value(row['Пат.почек, мет.виз.'], 'str')
        inspection.Microlithiasis = get_value(row['Микролитиаз'], 'str')
        inspection.Nephrolithiasis = get_value(row['Нефролитиаз'], 'str')
        inspection.Nephrocalcinosis = get_value(row['Нефрокальциноз'], 'str')
        inspection.Ckd = get_value(row['ХБП'], 'str')
        inspection.PatcnsDateOfObsl = get_value(row['Пат.ЦНС, дата обсл.'], 'datetime')
        inspection.PatcnsMetvisa = get_value(row['Пат.ЦНС, мет.виз.'], 'str')
        inspection.Psychoneuvrezab = get_value(row['Психоневр.заб.'], 'bool')
        inspection.Calcgm = get_value(row['Кальц.ГМ'], 'bool')
        inspection.CalcgmOther = get_value(row['Кальц.ГМ, другое'], 'str')
        inspection.PatcnsOther = get_value(row['Пат.ЦНС, другое'], 'str')
        inspection.PatorgvisionDateOfObsl = get_value(row['Пат.орг.зрения, дата обсл.'], 'datetime')
        inspection.PatorgvisionCataract = get_value(row['Пат.орг.зрения, катаракта'], 'str')
        inspection.PathorgvisionOther = get_value(row['Пат.орг.зрения, другое'], 'str')
        inspection.PathDigestiveTractSyndromeMalabsorbts = get_value(row['Пат.ЖКТ, синдр. мальабсорбц.'], 'bool')
        inspection.PathgastrointestinalTractUlcersBolZhel12pc = get_value(row['Пат.ЖКТ, язв. бол. жел./12ПК'], 'bool')
        inspection.PathgastrointestinalTractSyndromeshortkish = get_value(row['Пат.ЖКТ, синдр.коротк.киш.'], 'bool')
        inspection.PathGastrointestinalTractPatholliver = get_value(row['Пат.ЖКТ, патол.печени'], 'bool')
        inspection.CvdViolationrhythmAndWire = get_value(row['ССЗ. Наруш.ритма и провод.'], 'bool')
        inspection.CvdDilatatscardiomyopath = get_value(row['ССЗ. Дилатац.кардиомиопат.'], 'str')
        inspection.PatcostmouseFractures = get_value(row['Пат.кост.-мыш. Переломы'], 'str')
        inspection.OfThePatcostmouseDate1Transl = get_value(row['Пат.кост.-мыш. Дата 1 перел.'], 'datetime')
        inspection.PatcostmouseDate2Transl = get_value(row['Пат.кост.-мыш. Дата 2 перел.'], 'datetime')
        inspection.PatcostmouseDate3Transl = get_value(row['Пат.кост.-мыш. Дата 3 перел.'], 'datetime')
        inspection.PatcostmouseDate4Transl = get_value(row['Пат.кост.-мыш. Дата 4 перел.'], 'datetime')
        inspection.PatcostmouseLocalizationfractures = get_value(row['Пат.кост.-мыш. Локализ.перелома'], 'str')
        inspection.DxaDateOfObsl = get_value(row['DXA, дата обсл.'], 'str')
        inspection.DxaL2l4Zkr = get_value(row['DXA L2-L4 Z-кр.'], 'str')
        inspection.DxaL2l4Bmd = get_value(row['DXA L2-L4 BMD'], 'str')
        inspection.DxaFnZkr = get_value(row['DXA FN Z-кр.'], 'str')
        inspection.DxaFnBmd = get_value(row['DXA FN BMD'], 'str')
        inspection.DxaThZkr = get_value(row['DXA TH Z-кр.'], 'str')
        inspection.DxaThBmd = get_value(row['DXA TH BMD'], 'str')
        inspection.DxaTrZkr = get_value(row['DXA TR Z-кр.'], 'str')
        inspection.DxaTrBmd = get_value(row['DXA TR BMD'], 'str')
        inspection.DxaT33Zkr = get_value(row['DXA T33% Z-кр.'], 'str')
        inspection.DxaT33Bmd = get_value(row['DXA T33% BMD'], 'str')
        inspection.DxaTbs = get_value(row['DXA TBS'], 'str')
        inspection.ActFormsVitDAlfacalcidol = get_value(row['Акт. формы вит. Д - Альфакальцидол'], 'bool')
        inspection.ActFormsOfVitDCalcitriol = get_value(row['Акт. формы вит. Д - Кальцитриол'], 'bool')
        inspection.DailyDoseOfAlfacalcidol = str(get_value(row['Сут.доза Альфакальцидол'], 'float'))
        inspection.DailyDoseOfCalcitriol = get_value(row['Сут.доза Кальцитриол'], 'float')
        inspection.CalciumCarbonate = get_value(row['Карбонат кальция'], 'bool')
        inspection.CalciumCitrate = get_value(row['Цитрат кальция'], 'bool')
        inspection.CalciumGluconateIv = get_value(row['Глюконат кальция (в/в)'], 'bool')
        inspection.OtherCalciumSalts = get_value(row['Другие соли кальция'], 'bool')
        inspection.DailyDoseCalciumCarbonate = get_value(row['Сут.доза Карбонат кальция'], 'float')
        inspection.DailyDoseCalciumCitrate = get_value(row['Сут.доза Цитрат кальция'], 'float')
        inspection.DailyDoseCalciumGluconateIv = get_value(row['Сут.доза Глюконат кальция (в/в)'], 'float')
        inspection.DailyDoseOtherCalciumSalts = get_value(row['Сут.доза Другие соли кальция'], 'float')
        inspection.OtherLekmedication = get_value(row['Прочие лек.препараты'], 'str')
        inspection.Colecalciferol = get_value(row['Колекальциферол'], 'bool')
        inspection.MagnesiumPreparations = get_value(row['Препараты магния'], 'bool')
        inspection.PotassiumPreparations = get_value(row['Препараты калия'], 'bool')
        inspection.Hydrochlorothiazide = get_value(row['Гидрохлортиазид'], 'bool')
        inspection.RecombinantPth = get_value(row['Рекомбинантный ПТГ'], 'bool')
        inspection.TheUnitOfMeasurementOfPth = get_value(row['Единица измерения ПТГ'], 'str')
        inspection.DailyDoseOfColecalciferol = get_value(row['Сут.доза Колекальциферол'], 'float')
        inspection.DailyDoseOfPrepmagnesium = get_value(row['Сут.доза Преп.магния'], 'float')
        inspection.DailyDoseOfHydrochlorothiazide = get_value(row['Сут.доза Гидрохлортиазид'], 'float')
        inspection.DailyDoseOfTheRevpotassium = get_value(row['Сут.доза Преп.калия'], 'str')
        inspection.DailyDoseRecombinantPth = get_value(row['Сут.доза Рекомбинантный ПТГ'], 'float')
        inspection.InhibitprotonPump = get_value(row['Ингиб.протонной помпы'], 'bool')
        inspection.AntacidDrugs = get_value(row['Антацидные препараты'], 'bool')
        inspection.Glucocorticoids = get_value(row['Глюкокортикоиды'], 'bool')
        inspection.AntidepressantFluox = get_value(row['Антидепресс.(флуокс.)'], 'bool')
        inspection.Antimicrobialsrva = get_value(row['Противомикроб.ср-ва'], 'bool')
        inspection.Chemotherapy = get_value(row['Химиотерапия'], 'bool')
        inspection.Immunosuppressants = get_value(row['Иммуносупрессанты'], 'bool')
        inspection.LifeStatusOfThePatient = get_value(row['Жизненный статус пациента'], 'str')
        inspection.Comments = get_value(row['Комментарии'], 'str')
        inspection.DateOfDeparture = get_value(row['Дата выбытия'], 'str')
        inspection.CauseOfDeath = get_value(row['Причина смерти'], 'str')

        inspection.TherapyType = TherapyType.Irrational.value

        def second_part_of_therapy(i):
            return \
                    i.CalciumCarbonate or \
                    i.CalciumCitrate or \
                    i.OtherCalciumSalts or \
                    (i.CalciumCarbonate and i.Colecalciferol) or \
                    (i.CalciumCitrate and i.Colecalciferol) or \
                    (i.OtherCalciumSalts and i.Colecalciferol) or \
                    (i.CalciumCarbonate and i.MagnesiumPreparations and i.PotassiumPreparations) or \
                    (i.CalciumCitrate and i.MagnesiumPreparations and i.PotassiumPreparations) or \
                    (i.OtherCalciumSalts and i.MagnesiumPreparations and i.PotassiumPreparations) or \
                    (i.CalciumCarbonate and i.MagnesiumPreparations and i.PotassiumPreparations and i.Hydrochlorothiazide) or \
                    (i.CalciumCitrate and i.MagnesiumPreparations and i.PotassiumPreparations and i.Hydrochlorothiazide) or \
                    (i.OtherCalciumSalts and i.MagnesiumPreparations and i.PotassiumPreparations and i.Hydrochlorothiazide) or \
                    (i.CalciumCarbonate and i.MagnesiumPreparations) or \
                    (i.CalciumCitrate and i.MagnesiumPreparations) or \
                    (i.OtherCalciumSalts and i.MagnesiumPreparations) or \
                    (i.CalciumCitrate and i.MagnesiumPreparations and i.Hydrochlorothiazide) or \
                    (i.OtherCalciumSalts and i.MagnesiumPreparations and i.Hydrochlorothiazide) or \
                    (i.CalciumCarbonate and i.Hydrochlorothiazide) or \
                    (i.CalciumCitrate and i.Hydrochlorothiazide) or \
                    (i.OtherCalciumSalts and i.Hydrochlorothiazide)

        def check_no_data_therapy(i):
            return not (
                i.Colecalciferol or \
                i.MagnesiumPreparations or \
                i.PotassiumPreparations or \
                i.Hydrochlorothiazide or \
                i.RecombinantPth or \
                i.ActFormsVitDAlfacalcidol or \
                i.ActFormsOfVitDCalcitriol or \
                i.CalciumCarbonate or \
                i.CalciumCitrate or \
                i.CalciumGluconateIv or \
                i.OtherCalciumSalts
            )

        def try_parse_or_zero(input):
            try:
                return float(input)
            except:
                return 0

        if try_parse_or_zero(inspection.DailyDoseOfAlfacalcidol) == 0:
            inspection.ActFormsVitDAlfacalcidol = False
        else:
            inspection.ActFormsVitDAlfacalcidol = True

        if try_parse_or_zero(inspection.DailyDoseOfColecalciferol) == 0:
            inspection.Colecalciferol = False
        else:
            inspection.Colecalciferol = True

        if (inspection.ActFormsVitDAlfacalcidol and second_part_of_therapy(inspection)) or \
           (inspection.ActFormsOfVitDCalcitriol and second_part_of_therapy(inspection)):
            inspection.TherapyType = TherapyType.Rational.value

        if (check_no_data_therapy(inspection)):
            inspection.TherapyType = TherapyType.NoData.value

        def definition_type_by_calcium_total(inspection):
            inspection.CalciumTotalTypeTargetValue = False
            inspection.CalciumTotalType = None
            if inspection.CalciumTotal == '' or inspection.CalciumTotal is None or inspection.CalciumTotal == 'nan':
                return
            if 2.1 < float(inspection.CalciumTotal) < 2.3:
                inspection.CalciumTotalTypeTargetValue = True
                return
            if 2.15 < float(inspection.CalciumTotal) < 2.55:
                inspection.CalciumTotalType = CalciumType.Normocalcemia.value
            if float(inspection.CalciumTotal) < 2.15:
                inspection.CalciumTotalType = CalciumType.Hypocalcemia.value
            if float(inspection.CalciumTotal) > 2.55:
                inspection.CalciumTotalType = CalciumType.Hypercalcemia.value

        def definition_type_by_albuminadjusted_calcium(inspection):
            inspection.AlbuminadjustedCalciumTypeTargetValue = False
            inspection.AlbuminadjustedCalciumType = None
            if inspection.AlbuminadjustedCalcium == '' or inspection.AlbuminadjustedCalcium is None or inspection.AlbuminadjustedCalcium == 'nan':
                return
            if 2.1 < float(inspection.AlbuminadjustedCalcium) < 2.3:
                inspection.CalciumTotalTypeTargetValue = True
                return
            if 2.15 < float(inspection.AlbuminadjustedCalcium) < 2.55:
                inspection.AlbuminadjustedCalciumType = CalciumType.Normocalcemia.value
            if float(inspection.AlbuminadjustedCalcium) < 2.15:
                inspection.AlbuminadjustedCalciumType = CalciumType.Hypocalcemia.value
            if float(inspection.AlbuminadjustedCalcium) > 2.55:
                inspection.AlbuminadjustedCalciumType = CalciumType.Hypercalcemia.value

        def definition_type_by_calcium_ion(inspection):
            inspection.CalciumIonType = None
            if inspection.CalciumIon == '' or inspection.CalciumIon is None or inspection.CalciumIon == 'nan':
                return
            if 2.15 < float(inspection.CalciumIon) < 2.55:
                inspection.CalciumIonType = CalciumType.Normocalcemia.value
            if float(inspection.CalciumIon) < 2.15:
                inspection.CalciumIonType = CalciumType.Hypocalcemia.value
            if float(inspection.CalciumIon) > 2.55:
                inspection.CalciumIonType = CalciumType.Hypercalcemia.value

        definition_type_by_calcium_total(inspection)
        definition_type_by_albuminadjusted_calcium(inspection)
        definition_type_by_calcium_ion(inspection)

        if inspection.Id is None:
            inspection.Id = uuid.uuid4()
            db.add(inspection)
            added_inspections += 1

        total_processed_inspections += 1

        inspection.GFRReduction = False

        def try_parse_or_none(input):
            try:
                return float(input)
            except:
                return None

        if (try_parse_or_none(inspection.SkfByFormschwartz) != None and try_parse_or_none(inspection.SkfByFormschwartz) < 60) or \
                (try_parse_or_none(inspection.GfrByEpi) != None and try_parse_or_none(inspection.GfrByEpi) < 60):
            inspection.GFRReduction = True
        elif try_parse_or_none(inspection.SkfByFormschwartz) == None and try_parse_or_none(inspection.GfrByEpi) == None:
            inspection.GFRReduction = None

    db.commit()
    return added_patients, added_inspections, total_processed_inspections


async def parse_body(request: Request):
    data: bytes = await request.body()
    return data
