"""
'add-create-ai-report-table'
"""

from yoyo import step

__depends__ = {'20240916_01_HoESn-create-db'}

steps = [
step('CREATE TABLE public."AiReportResults"("Id" uuid, "CreateAt" timestamp without time zone, "Request" character varying, "IsSuccess" boolean, "Sql" character varying, "Result" character varying, PRIMARY KEY ("Id"));')
]
