#! /usr/bin/python3
import time
import json
import pymysql
from bs4 import BeautifulSoup
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from email.utils import parseaddr
import logging
import configparser
import datetime
import os
import requests
import re
import random
import numpy as np # pip install numpy
import cv2   # pip install opencv-python
from pillow_heif import open_heif  # pip install pillow-heif
import cgi
import cgitb; cgitb.enable()
print ("Content-type: text/html")
print ("")

s = requests.Session()
api_access_token = ""
partner_id = ""
is_appfolio_enabled = ""

config = configparser.RawConfigParser()
# config.read('/home/flock/PycharmProjects/sample1/s3_project/django_app/django_application/config.ini')
# config.read('/home/forge/scripts/cornex_scripts/config.ini')
# config.read('/home/forge/mgwh.lula.io/django_app/config.ini')
config.read('/home/forge/mgwh.lula-dev.com/django_application/config.ini')
# config.read('/home/forge/mgwh.lula.io/django_application/config.ini')

appfolio_username = config['appfolio_credentials']['username']
appfolio_password = config['appfolio_credentials']['password']
image_file_path = config["image_path"]["appfolio_path_name"]
external_job_source_id = config["external_job_source_id"]["externaljobsourceid"]

def log_file():
    log_day = datetime.datetime.now().strftime('%d-%m-%Y')
    log_name = '/var/log/appfolio_script_logs/' + log_day + '_appfolio_mail_log.log'
    # log_name = log_day + '_vendorcafe_log.log'
    return log_name

def send_email_lula(message, subject_message):
    try:
        msg = MIMEMultipart()
        msg.attach(MIMEText(message))
        msg['Subject'] = subject_message
        server = smtplib.SMTP('smtp.mailgun.org', 587)
        server.starttls()
        server.login('postmaster@mg.lula.life', 'ca9e0ae19e69b0318be84e27a38d7996')
        recipients = ['aaaaaaaaaaa', 'vvvvvvvvvvvvvvvvvv']
        msg["To"] = ",".join(recipients)
        bcc = ['aaaaaaaaaaaaaaaa', 'vvvvvvvvvvvvvvvvvvv']
        to_address = recipients + bcc
        server.sendmail('info@lula.life', to_address, msg.as_string())
        print("Alert mail sent successfully -->")
    except Exception as e:
        print("Unable to send email due to the loss of network")
        logging.info("Sending Email Failure {}".format(e))

def send_email_programmers(message, subject_message):
    try:
        msg = MIMEMultipart()
        msg.attach(MIMEText(message))
        msg['Subject'] = subject_message
        server = smtplib.SMTP('smtp.mailgun.org', 587)
        server.starttls()
        server.login('postmaster@mg.lula.life', 'ca9e0ae19e69b0318be84e27a38d7996')
        recipients = ['ajay.krishna@quintetsolutions.com']
        bcc = ['krishnaajay998@gmail.com']
        to_address = recipients + bcc
        server.sendmail('info@lula.life', to_address, msg.as_string())
        print("Alert mail sent successfully")
    except Exception as e:
        print("Unable to send email due to the loss of network")
        logging.info("Sending Email Failure {}".format(e))

def sendNewCustomerAccountAlert(partner_id,partner_name):
    try:
        subject = "[Immediate Action Required] New Customer Account added, " + str(partner_name) + "in AppFolio"
        email_content = """
        A new customer account just got added in AppFolio which require following action.

        1.Sync the connection between PM software and the scraper in the customer account.
         PM Software new Customer Account name : """ + str(partner_name) + """
         Lula Customer Listing : https://lula.io/luladmin/customers
         """
        msg = MIMEMultipart()
        msg.attach(MIMEText(email_content))
        msg['Subject'] = subject
        server = smtplib.SMTP('smtp.mailgun.org', 587)
        server.starttls()
        server.login('postmaster@mg.lula.life', 'ca9e0ae19e69b0318be84e27a38d7996')
        recipients = ['ajay.krishna@quintetsolutions.com']
        bcc = ['krishnaajay998@gmail.com']
        to_address = recipients + bcc
        server.sendmail('info@lula.life', to_address, msg.as_string())
        print("Alert mail sent successfully")
    except Exception as e:
        print("Unable to send email due to the loss of network")
        logging.info("Sending Email Failure {}".format(e))

def create_file(message_id,email_content):
    try:
        file_creation_date = datetime.datetime.now().strftime('%d-%m-%Y')
        file_name = "/home/forge/mgaf.lula-dev.com/file_content/" + file_creation_date + "_" + message_id + ".txt"
        # file_name = '/home/flock/PycharmProjects/sample1/s3_project/file_contents/' + file_creation_date + "_" + message_id + ".txt"
        f = open(file_name, "w")
        f.write(email_content)
        f.close()
    except Exception as e:
        print("Unable create file for email_content: "+str(e))
        logging.info("Unable create file for email_content: "+str(e))

def get_partner_id(customer_reference_id,job_url,portfolio_name):
    try:
        cursor.execute("SELECT partner_id FROM partner_integrations WHERE customer_reference_id=%s AND external_job_source_id=%s",
                       (customer_reference_id,external_job_source_id))
        datas = cursor.fetchone()
        if datas:
            global partner_id
            partner_id = datas["partner_id"]
            logging.info('Got Partner Id: {}'.format(partner_id))
            return partner_id
        else:
            print("Issue in getting partner_id for customer_reference_id: " + str(customer_reference_id))
            send_email_programmers(
                "No data present for getting partner_id for customer_reference_id: " + str(customer_reference_id),
                "Appfolio: Unable to get partner id for job_url: " + str(job_url))
            created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("INSERT INTO partner_integrations (external_job_source_id,customer_reference_id,customer_name,created_at) VALUES (%s,%s,%s,%s)",(external_job_source_id,customer_reference_id,portfolio_name,created_at))
            logging.info(
                "No data present for getting partner_id for customer_reference_id: " + str(customer_reference_id))
    except Exception as e:
        print(e)
        send_email_programmers(
            "Issue in getting partner_id for customer_reference_id: " + str(customer_reference_id) + " Error: " + str(
                e), "Appfolio: Unable to get partner id for job_url: " + str(job_url))
        logging.info(
            "Issue in getting partner_id for customer_reference_id: " + str(customer_reference_id) + " Error: " + str(
                e))
        exit(0)


def get_access_token(partner_id,job_url):
    try:
        cursor.execute("SELECT api_access_token,is_appfolio_enabled FROM partners WHERE partner_id=%s", (partner_id,))
        datas = cursor.fetchone()
        print(datas)
        if datas:
            global api_access_token
            api_access_token = datas["api_access_token"]
            global is_appfolio_enabled
            is_appfolio_enabled = datas["is_appfolio_enabled"]
            logging.info('Got Api Access Token: {}'.format(api_access_token))
            print(api_access_token)
            print("is_appfolio_enabled: "+str(is_appfolio_enabled))
            return api_access_token
        else:
            print("Issue in getting api_access_token for partner_id: " + str(partner_id))
            send_email_programmers(
                "No data present for getting api_access_token for partner_id: " + str(partner_id),
                "Appfolio: Unable to get partner id for job_url: " + str(job_url))
            logging.info(
                "No data present for getting api_access_token for partner_id: " + str(partner_id))
    except Exception as e:
        print(e)
        send_email_programmers("Issue in getting api_access_token for partner_id: " + str(partner_id)+ "Error: "+str(e),
                               "Appfolio: Unable to get partner id for job_url: "+str(job_url))
        logging.info("Issue in getting api_access_token for partner_id: "+str(partner_id))
        exit(0)

def appfilio_login():
    payload = {
        'username': appfolio_username,
        'password': appfolio_password,
        'vhost': "vendor",
        'idp_type': "vendor",
        'client_id': "passport-frontend",
        'grant_type': "password",
        'vhostless': "false",
        'require_reverification': "true",
        'sync_phone_numbers': "true",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
    }
    try:
        login_session = s.post('https://oauth.appf.io/oauth/token', data=payload, headers=headers)
        login_obj = login_session.text
        print(login_obj)
        up_obj = json.loads(login_obj)
        access_token = up_obj["access_token"]
        return access_token
    except Exception as e:
        print("Unable to login to appfolio website: "+str(e))
        logging.info("Unable to login to appfolio website: "+str(e))

def post_structured_datas(priority_datas,work_order_details_json_data,job_url,job_reference_id):
    try:
        if "numberForDisplay" in work_order_details_json_data.keys():
            refernce_no = work_order_details_json_data["numberForDisplay"]
            print("external_job_source_details: reference_no: "+str(refernce_no))
            logging.info("external_job_source_details: reference_no: "+str(refernce_no))
        else:
            refernce_no = ""
            logging.info("Empty external_job_source_details: reference_no: " + str(refernce_no))

        priority_datas.update({"external_job_source_details": {"reference_no": refernce_no,
                                                               "external_job_link": job_url,
                                                               "external_job_source": "appfolio"}})

        if "address" in work_order_details_json_data.keys():
            address = work_order_details_json_data["address"]
            if "address1" in address.keys():
                street = address["address1"]
                print("street: "+str(street))
                logging.info("street: "+str(street))
            else:
                street = ""
                logging.info("Empty street: " + str(street))

            if "address2" in address.keys():
                apartment_number = address["address2"]
                print("apartment_number: "+str(apartment_number))
                logging.info("apartment_number: "+str(apartment_number))
            else:
                apartment_number = ""
                logging.info("Empty apartment_number: " + str(apartment_number))

            if apartment_number == "":
                if "propertyOrUnitName" in address.keys():
                    apartment_number = address["propertyOrUnitName"]
                    print("apartment_number:propertyOrUnitName: "+str(apartment_number))
                    logging.info("apartment_number:propertyOrUnitName: "+str(apartment_number))
                else:
                    apartment_number = ""
                    print("apartment_number:propertyOrUnitName: " + str(apartment_number))
                    logging.info("Empty apartment_number:propertyOrUnitName: " + str(apartment_number))

            if "city" in address.keys():
                city = address["city"]
                print("city: "+str(city))
                logging.info("city: "+str(city))
            else:
                city = ""
                logging.info("Empty city: "+str(city))

            if "state" in address.keys():
                state = address["state"]
                print("state: "+str(state))
                logging.info("state: "+str(state))
            else:
                state = ""
                logging.info("Empty state: " + str(state))

            if "zipCode" in address.keys():
                zipCode = address["zipCode"]
                print("zipCode: " + str(zipCode))
                logging.info("zipCode: " + str(zipCode))
            else:
                zipCode = ""
                logging.info("Empty zip_code: " + str(zipCode))

            priority_datas.update({"property_details": {
                "address": {"state": state, "city": city, "zip_code": zipCode,
                            "street": street,"apartment_number":apartment_number}}})


        priority_datas["job_reference_id"] = job_reference_id
        logging.info("Priority data: "+str(job_reference_id))

        if "description" in work_order_details_json_data.keys():
            description = work_order_details_json_data["description"]
            print("description: " + str(description))
            logging.info("description: " + str(description))
        else:
            description = ""
            logging.info("Empty description: " + str(description))

        priority_datas["description"] = description

        if "vendorInstructions" in work_order_details_json_data.keys():
            description_2 = work_order_details_json_data["vendorInstructions"]
            print("vendorInstructions: " + str(description_2))
            logging.info("vendorInstructions: " + str(description_2))
        else:
            description_2 = ""
            logging.info("Empty vendorInstructions: " + str(description_2))

        priority_datas["description_2"] = description_2

        if "specialInstructions" in work_order_details_json_data.keys():
            description_3 = work_order_details_json_data["specialInstructions"]
            print("specialInstructions: " + str(description_3))
            logging.info("specialInstructions: " + str(description_3))
        else:
            description_3 = ""
            logging.info("Empty specialInstructions: " + str(description_3))
        priority_datas["description_3"] = description_3

        if "estimate" in work_order_details_json_data.keys():
            print(work_order_details_json_data["estimate"])
            if work_order_details_json_data["estimate"] != None:
                if "id" in work_order_details_json_data["estimate"]:
                    estimate_id = work_order_details_json_data["estimate"]["id"]
                    print("estimate_id: "+str("yes"))
                    logging.info("estimate_id: "+str("yes"))
                    priority_datas["need_quote"] = "yes"
                else:
                    print("estimate_id: "+str("no"))
                    logging.info("estimate_id: "+str("no"))
                    priority_datas["need_quote"] = "no"
            else:
                print("estimate_id: " + str("no"))
                logging.info("No key estimate :estimate_id: " + str("no"))
                priority_datas["need_quote"] = "no"

        checks_mkready = re.search("^.*(make ready|make\s*[\_\-\s]*\s*ready|make-ready|:make_ready).*$",
                                   str(work_order_details_json_data))
        if checks_mkready:
            service_category = "make-ready"
            print("service category: " + str(service_category))
            logging.info("service category: " + str(service_category))
            priority_datas["service_category"] = "make-ready"
        else:
            service_category = "unknown"
            print("service category: " + str(service_category))
            logging.info("service category: " + str(service_category))
            priority_datas["service_category"] = "unknown"

        priority_datas["nte_amount"] = ""

        if "tenantIsUrgent" in work_order_details_json_data.keys():
            tenantIsUrgent = work_order_details_json_data["tenantIsUrgent"]
            if tenantIsUrgent == 1:
                is_emergency = 1
                priority_datas["is_emergency"] = "yes"
                print("is_emergency: "+str(is_emergency))
                logging.info("is_emergency: "+str(is_emergency))
            else:
                is_emergency = 0
                priority_datas["is_emergency"] = "no"
                print("is_emergency: " + str(is_emergency))
                logging.info("is_emergency: " + str(is_emergency))
        else:
            priority_datas["is_emergency"] = "no"
            logging.info("No key tenantIsUrgent is_emergency: " + str("no"))

        primary_tenant_name = ""
        primary_tenant_email = ""
        primary_tenant_phone_no = ""
        if "primaryTenant" in work_order_details_json_data.keys():
            primaryTenant = work_order_details_json_data["primaryTenant"]
            if primaryTenant == {} or primaryTenant == []:
                print("No tenant details")
                logging.info("No tenant details")
            else:
                if "name" in primaryTenant.keys():
                    primary_tenant_name = primaryTenant["name"]
                    print("primary_tenant_name: " + str(primaryTenant["name"]))
                    logging.info("primary_tenant_name: " + str(primaryTenant["name"]))

                if config.has_option("mode_dev","phone_number"):
                    logging.info("Mode dev")
                    phone_number = [config["mode_dev"]["phone_number"]]
                    logging.info("Dev phone no: "+str(phone_number))
                    primary_tenant_phone_no = phone_number
                    email_data = config["mode_dev"]["email"]
                    logging.info("Config dev email_data: "+str(email_data))
                    num = random.randrange(100, 10 ** 3)
                    email = 'lula' + str(num).join(re.split(r"\blula\b", email_data))
                    primary_tenant_email = email
                    logging.info("primary_tenant_email dev: "+str(primary_tenant_email))
                else:
                    if "email" in primaryTenant.keys():
                        primary_tenant_email = primaryTenant["email"]
                        print("primary_tenant_email: " + str(primaryTenant["email"]))
                        logging.info("primary_tenant_email: " + str(primaryTenant["email"]))
                    primary_tenant_phone_no = []
                    if "phoneNumbers" in primaryTenant.keys():
                        phone_number_detail = primaryTenant["phoneNumbers"]
                        for ph_no_detail in phone_number_detail:
                            if "number" in ph_no_detail.keys():
                                number = ph_no_detail["number"]
                                ph_no = re.sub("[^0-9]", "", number)
                                primary_tenant_phone_no.append(ph_no)
                    print("primary_tenant_phone_no: " + str(primary_tenant_phone_no))
                    logging.info("primary_tenant_phone_no: " + str(primary_tenant_phone_no))

        priority_datas.update({
            "tenant_details": [
                {"phone": primary_tenant_phone_no, "email": str(primary_tenant_email),
                 "name": str(primary_tenant_name)}]}
        )


        if "otherTenants" in work_order_details_json_data.keys():
            otherTenants = work_order_details_json_data["otherTenants"]
            if otherTenants == {} or otherTenants == []:
                print("No other tenant details")
                logging.info("No other tenant details")
            else:
                otherTenants_name = ""
                otherTenants_email = ""
                secondary_tenant_phone_no = ""
                for secondary_tenant in otherTenants:
                    if "name" in secondary_tenant.keys():
                        otherTenants_name = secondary_tenant["name"]
                        print("otherTenants_name: " + str(secondary_tenant["name"]))
                        logging.info("otherTenants_name: " + str(secondary_tenant["name"]))

                    if config.has_option("mode_dev", "phone_number"):
                        logging.info("Mode dev others tenant")
                        phone_number = [config["mode_dev"]["phone_number"]]
                        logging.info("Dev others tenant phone no: " + str(phone_number))
                        secondary_tenant_phone_no = phone_number
                        email_data = config["mode_dev"]["email"]
                        logging.info("Config dev others tenant email_data: " + str(email_data))
                        num = random.randrange(100, 10 ** 3)
                        email = 'lula' + str(num).join(re.split(r"\blula\b", email_data))
                        otherTenants_email = email
                        logging.info("secondary_tenant_email dev: " + str(primary_tenant_email))

                    else:
                        if "email" in secondary_tenant.keys():
                            otherTenants_email = secondary_tenant["email"]
                            print("otherTenants_email: " + str(secondary_tenant["email"]))
                            logging.info("otherTenants_email: " + str(secondary_tenant["email"]))
                        secondary_tenant_phone_no = []
                        if "phoneNumbers" in secondary_tenant.keys():
                            phone_number_detail = secondary_tenant["phoneNumbers"]
                            for ph_no_detail in phone_number_detail:
                                if "number" in ph_no_detail.keys():
                                    number = ph_no_detail["number"]
                                    ph_no = re.sub("[^0-9]", "", number)
                                    secondary_tenant_phone_no.append(ph_no)
                        print("secondary_tenant_phone_no: " + str(secondary_tenant_phone_no))
                        logging.info("secondary_tenant_phone_no: " + str(secondary_tenant_phone_no))

                    if otherTenants_name != "":
                        priority_datas["tenant_details"].append(
                            {"phone": secondary_tenant_phone_no, "email": str(otherTenants_email), "name": str(otherTenants_name)}
                        )

        property_manager_count = 0
        property_manager_email = ""
        if "assignedUsers" in work_order_details_json_data.keys():
            assignedUsers = work_order_details_json_data["assignedUsers"]
            if assignedUsers == [] or assignedUsers == {}:
                print("No assignedUsers")
                logging.info("No assignedUsers")
            else:
                for users in assignedUsers:
                    if "email" in users.keys():
                        logging.info("Increase count property_manager_count fount email key")
                        property_manager_count = property_manager_count+1

            if property_manager_count>0:
                if "email" in assignedUsers[0].keys():
                    property_manager_email = assignedUsers[0]["email"]
                    print("property_manager_details_email: " + str(property_manager_email))
                    logging.info("property_manager_details_email: " + str(property_manager_email))
                else:
                    property_manager_email = ""
                    logging.info("No email key in assignedUsers")
            else:
                property_manager_email = ""
                logging.info("Zero property_manager_count")

        priority_datas.update({
            "property_manager_details": [
                {"email": str(property_manager_email)}]})

    except Exception as e:
        print("Something issue in creating the structured json: "+str(e))
        logging.info("Something issue in creating the structured json: "+str(e))
        send_email_programmers("Unable to form structured json from the data: " + str(e),
                               "Appfolio: Failed to form structured json from the data")






def print_data(work_order_details_json_data,job_url):
    print("==================Main Data===============================")

    if "id" in work_order_details_json_data.keys():
        job_refernce_id = work_order_details_json_data["id"]
        print("job_refernce_id: "+str(job_refernce_id))
    if "description" in work_order_details_json_data.keys():
        description = work_order_details_json_data["description"]
        print("description: " + str(description))
    if "vendorInstructions" in work_order_details_json_data.keys():
        description_2 = work_order_details_json_data["vendorInstructions"]
        print("vendorInstructions: " + str(description_2))
    if "specialInstructions" in work_order_details_json_data.keys():
        description_3 = work_order_details_json_data["specialInstructions"]
        print("specialInstructions: " + str(description_3))
    if "estimate" in work_order_details_json_data.keys():
        if work_order_details_json_data["estimate"] != None:
            if "id" in work_order_details_json_data["estimate"]:
                estimate_id = work_order_details_json_data["estimate"]["id"]
                print("estimate_id: "+str("yes"))
            else:
                print("estimate_id: "+str("no"))
        else:
            print("estimate_id: " + str("no"))

    checks_mkready = re.search("^.*(make ready|make\s*[\_\-\s]*\s*ready|make-ready|:make_ready).*$", str(work_order_details_json_data))
    if checks_mkready:
        service_category = "make-ready"
        print("service category: "+str(service_category))
    else:
        service_category = "unknown"
        print("service category: "+str(service_category))


    nte_amount = ""
    print("nte_amount: "+str(nte_amount))

    if "tenantIsUrgent" in work_order_details_json_data.keys():
        tenantIsUrgent = work_order_details_json_data["tenantIsUrgent"]
        if tenantIsUrgent == 1:
            is_emergency = 1
            print("is_emergency: "+str("yes"))
        else:
            is_emergency = 0
            print("is_emergency: " + str("no"))

    print("====================External Job source Details=======================================")

    if "numberForDisplay" in work_order_details_json_data.keys():
        refernce_no = work_order_details_json_data["numberForDisplay"]
        print("external_job_source_details: reference_no: "+str(refernce_no))

    print("external_job_source_details: job_url: " + str(job_url))
    print("external_job_source_details: external_job_source: " + str("appfolio"))

    print("====================Property details=========================================")

    if "address" in work_order_details_json_data.keys():
        address = work_order_details_json_data["address"]
        if "address1" in address.keys():
            street = address["address1"]
            print("street: "+str(street))

        if "address2" in address.keys():
            apartment_number = address["address2"]
            print("apartment_number: "+str(apartment_number))

        if "city" in address.keys():
            city = address["city"]
            print("city: "+str(city))

        if "state" in address.keys():
            state = address["state"]
            print("state: "+str(state))

        if "zipCode" in address.keys():
            zipCode = address["zipCode"]
            print("zipCode: " + str(zipCode))

    print("====================Tenant details=========================================")

    if "primaryTenant" in work_order_details_json_data.keys():
        primaryTenant = work_order_details_json_data["primaryTenant"]
        if primaryTenant == {} or primaryTenant == []:
            print("No tenant details")
        else:
            if "name" in primaryTenant.keys():
                print("Name: "+str(primaryTenant["name"]))
            if "email" in primaryTenant.keys():
                print("Email: "+str(primaryTenant["email"]))
            phone_no = []
            if "phoneNumbers" in primaryTenant.keys():
                phone_number_detail = primaryTenant["phoneNumbers"]
                for ph_no_detail in phone_number_detail:
                    if "number" in ph_no_detail.keys():
                        number = ph_no_detail["number"]
                        ph_no = re.sub("[^0-9]", "", number)
                        phone_no.append(ph_no)
            print("Phone no: "+str(phone_no))

    if "otherTenants" in work_order_details_json_data.keys():
        otherTenants = work_order_details_json_data["otherTenants"]
        if otherTenants == {} or otherTenants == []:
            print("No other tenant details")
            logging.info("No other tenant details")
        else:
            for secondary_tenant in otherTenants:
                if "name" in secondary_tenant.keys():
                    print("Name: " + str(secondary_tenant["name"]))
                if "email" in secondary_tenant.keys():
                    print("Email: " + str(secondary_tenant["email"]))
                phone_no = []
                if "phoneNumbers" in secondary_tenant.keys():
                    phone_number_detail = secondary_tenant["phoneNumbers"]
                    for ph_no_detail in phone_number_detail:
                        if "number" in ph_no_detail.keys():
                            number = ph_no_detail["number"]
                            ph_no = re.sub("[^0-9]", "", number)
                            phone_no.append(ph_no)
                print("Phone no: " + str(phone_no))

    print("=============================================================")

    property_manager_count = 0
    if "assignedUsers" in work_order_details_json_data.keys():
        assignedUsers = work_order_details_json_data["assignedUsers"]
        if assignedUsers == [] or assignedUsers == {}:
            print("No assignedUsers")
        else:
            for users in assignedUsers:
                if "email" in users.keys():
                    property_manager_count = property_manager_count+1

        if property_manager_count>0:
            property_manager_email = assignedUsers[0]["email"]
            print("property_manager_details_email: " + str(property_manager_email))

    print("=============================================================")

def insert_scrapper_job_error_logs(id, job_url, job_stage, error_code, error_message, work_orders, api_response_data,
                                   post_json_data,customer_reference_id):
    try:

        cursor.execute(
            "INSERT INTO scrapper_jobs_error_logs (external_job_source_id,job_reference_id,job_url,job_stage,error_code,error_message,last_updated,job_data,api_response_data,payload,customer_reference_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (external_job_source_id, id, job_url, job_stage, error_code, error_message, start_time, work_orders, api_response_data,
             post_json_data,customer_reference_id))

    except Exception as e:
        print("Unable to insert into scrapper error logs table")
        logging.info("Unable to insert into scrapper error logs table " + str(e))
        send_email_programmers("Unable to insert into scrapper jobs table: "+str(e),
                               "Appfolio: Unable to insert into scrapper error logs table for job_reference_no: " + str(
                                   id))


def post_image_data(file_path, job_reference_id, subject):
    try:
        payload = {"note": ""}
        content = {
            "files[]": open(file_path, 'rb'),
        }
        headers = {
            'accept': 'application/json',
            "Authorization": "Bearer " + api_access_token
        }
        post_url = config['settings']['lula_post_url']

        print("POst image Job reference id: " + str(job_reference_id))
        logging.info("POst image Job reference id: " + str(job_reference_id))

        p = s.post(post_url + "/" + str(job_reference_id) + '/notes',
                   files=content, headers=headers, data=payload)

        image_posted_response = ""
        image_posted_response = p.text
        print(image_posted_response)
        posted_json_response = ""
        posted_json_response = p.json()
        logging.info("Json image post data: " + str(posted_json_response))

        if "message" in posted_json_response.keys():
            if posted_json_response["message"] != "Note added successfully.":
                logging.info("failed to post image-->for job_reference_no " + str(job_reference_id))
                subject_msg = "Appfolio: Failed to upload image [Subject: " + str(subject) + "] WO#" + str(
                    job_reference_id)
                send_email_programmers(image_posted_response, subject_msg)
            else:
                print("Image posted successfully")
                logging.info("Image posted successfully with job_reference_no: " + str(job_reference_id))
                subject_msg = "Appfolio: New Image posted [Subject: " + str(subject) + "] WO#" + str(job_reference_id)
                send_email_programmers(image_posted_response, subject_msg)
        else:
            print("No message found......")
            logging.info("No message found for job_reference_no: " + str(job_reference_id))
            subject_msg = "Appfolio: Failed to upload image  [Subject: " + str(subject) + "] WO#" + str(
                job_reference_id) + " No message found"
            send_email_programmers(image_posted_response, subject_msg)

        return image_posted_response

    except Exception as e:
        print("Unable to post image: " + str(e))
        logging.info("Unable to post data -- for WO# " + str(job_reference_id))
        subject_msg = "Appfolio: Failed to add job something wrong [Subject: " + str(subject) + "] WO#: " + str(
            job_reference_id) + " unable to post"
        send_email_programmers("Failed to add job something wrong " + str(e), subject_msg)

        return "Something wrong Unable to post"

def convert_heic_to_png(file_path):
    try:
        heif_file = open_heif(file_path, convert_hdr_to_8bit=False, bgr_mode=True)
        np_array = np.asarray(heif_file)
        convert_path = file_path+".png"
        cv2.imwrite(convert_path, np_array)
        return convert_path
    except Exception as e:
        logging.info("Error: convert_heic_to_png-->"+str(e))
        send_email_programmers("Error: convert_heic_to_png","Appfolio: Error: convert_heic_to_png")


def download_image_from_urls(urls, subject, job_reference_id):
    count = 0
    for each_url in urls:
        count = count + 1
        image_extension = "jpg"
        try:
            get_image_data = requests.get(each_url)
            print(get_image_data)
            content_disposition = get_image_data.headers['content-disposition']
            print(content_disposition)
            my_filename41 = re.search('filename="(\S+)"', content_disposition)
            if my_filename41:
                my_filename = my_filename41.group(1)
                print("Your filename: "+str(my_filename41.group(1)))
                logging.info("Your filename: "+str(my_filename41.group(1)))
                logging.info("Content dispostion: " + str(content_disposition))
                image_extension_search = re.search("\.([^\.]+)$", my_filename)
                print(image_extension_search)
                logging.info("Image extension search: " + str(image_extension_search))
                if image_extension_search:
                    image_extension = image_extension_search.group(1)
                    logging.info("Image extension group: " + str(image_extension))
                    print("Your image extension: " + str(image_extension))

                if image_extension:
                    print("Its image extrension: "+str(image_extension.lower()))
                    file_name = image_file_path + "_" + str(job_reference_id) + "_" + str(partner_id) + "_" + str(
                        count) + "." + image_extension.lower()
                    logging.info("Your file_name: " + str(file_name))
                    try:
                        open(file_name, "wb").write(get_image_data.content)
                        print('Image sucessfully Downloaded: ' + str(file_name))
                        logging.info('Image sucessfully Downloaded: ' + str(file_name))
                        if "heic" in image_extension.lower():
                            file_path = convert_heic_to_png(file_name)
                            print(str(file_path)+"heic_to_png--> file path")
                            logging.info("heic_to_png-->" + str(file_path))
                            ps_img_response = post_image_data(file_path, job_reference_id, subject)
                            print(ps_img_response)
                            logging.info(
                                "Heic Image Post response from the lula " + str(ps_img_response) + " for Job_reference_id: " + str(
                                    job_reference_id))
                        else:
                            ps_img_response = post_image_data(file_name, job_reference_id, subject)
                            print(ps_img_response)
                            logging.info(
                                "Post response from the lula " + str(ps_img_response) + " for po_number: " + str(
                                    job_reference_id))
                    except Exception as e:
                        logging.error(
                            'Error: Unable to download image and store in filepath ' + str(file_name) + " Error: " + str(
                                e))
                        send_email_programmers(
                            "Error: Unable to download image and store in filepath " + str(file_name) + "and job_refernce_id: "+str(job_reference_id),
                            "Appfolio: [Subject: " + str(
                                subject) + "] Unable to download image " + str(
                                each_url) + " WO# " + str(job_reference_id))
            else:
                print("Unable to the file name: "+str(content_disposition))
                logging.info("Unable to the file name: "+str(content_disposition))
                send_email_programmers("Unable to the file name: "+str(content_disposition)+" each url: "+str(each_url),"Appfolio: [Subject: " + str(
                    subject) + "] Unable to the file name " + str(
                    each_url) + " WO# " + str(job_reference_id))


        except Exception as e:
            logging.error(
                'Error: Url request Unable to download image from url request ' + str(urls) + " Error: " + str(e))
            send_email_programmers(
                "Error: Url request Unable to download image from url request" + str(job_reference_id),
                "Appfolio: [Subject: " + str(
                    subject) + "] Unable to download image for url request " + str(
                    each_url) + " WO# " + str(job_reference_id))

    #### Remove all files from directory ####
    for f in os.listdir(image_file_path):
        os.remove(os.path.join(image_file_path, f))
    ###########################################


def get_image_urls(work_order_details_data, subject, job_reference_id):
    if "tenantUploadedImages" in work_order_details_data.keys():
        print("Image url founded sucessfully")
        logging.info("Image url founded sucessfully")
        tenant_urls = work_order_details_data["tenantUploadedImages"]
        urls = []
        if tenant_urls == []:
            print("No urls found")
            logging.info("No urls found")
        else:
            for ivd_url in tenant_urls:
                if ivd_url["url"]:
                    url = ivd_url["url"]
                    logging.info("Your url: " + str(url))
                    urls.append(url)
            print(urls)
            download_image_from_urls(urls, subject, job_reference_id)
    else:
        print("Image url doesnt found successfully")
        logging.info("Image url doesnt found successfully")


def get_data(email_html_content,subject):
    soup = BeautifulSoup(email_html_content, 'html.parser')
    # url = soup.select_one("a", href=re.compile("sg.appfolio.com/ls/click"))
    url = soup.find("a", text=re.compile("view work order|view this work order", re.IGNORECASE))
    print(url)
    logging.info("Your url data: "+str(url))
    if url:
        email_url = url['href']
        print("Email_url: "+str(email_url))
        logging.info("Email_url: "+str(email_url))
        # email_url = "https://sg.appfolio.com/ls/click?upn=SEmYLt35t2PsBK7zrZwyvHbpIwYmxl3AHlcKVr0mL-2B35jaltAN8BhSO2nNWm88URy9tYfApGbzwffp8DNJVKT8G4Tk-2BH3ahwLX1RPzuM9JLk3RB7ZzP2R91UdclbZM1V3YaKm6dSR-2BMvh5Um3crk80SoyJV02j-2FjyL2QsCRP4UVO47XuTI2HLJC0GalF5gNw1WbpNEf9vulNDeLVrPWjX-2BHjhCExO5GR76Bb7s4XupBFembEkh6ESJYWmM6k2ogFbue7vVB6Hw3nksMwDfYXQD-2FHyBW7Fen9M-2BYjwWalCyZ9eegFztNLfg7K-2BIb6IlH2DPbttTtMB8hEwNaQfPfwZRirkumAFgOQgc5wnsIFITfPzDJkCkPoRWRmaSCq1-2BnaPiexV50rS-2B-2F7U2s07CVw5TreAcARc-2Bpe8MqDqsP4aNrc9QLdk1pb-2B3zdRZ-2FUcXMVFIukNdxakJ3nRr6m5JFlmC6IItB4PfjW8x3As-2B0AnHdV6W5sMBL0skyFVJoa6Mp7hpFuZW3-2Fakm7FPYuo49-2B-2F7AKDbTX1laeMGwTh6eybqZu0tSsR-2BeEjD4VcL9pSSGaWtbMC-2BE6e43a9ffMgeufO2vm8JMRldJzJ0LAC1fC2LKdus-2BwgWMneUWpSLfT9IXJm-2BFHhKDRNSAcj8NETxcB5NLcS9NNt08YmqFwTT3sMrhyTeotAQ6KN3tJsIRgBK6dQaidD-2BLE6Xy2Cq6sB8NMf9JiF1UOd-2BD7DapJ6MgwCIiUc8yGzJ-2BLMVVlaYSQFWCbIOMbfuIu66pwC8HWvJm46qxW-2BjD9Vl0nQOF822IvWnNKDMYdaDMHIxWDFAGOd4Pow-2FuTbip2x0bXaxMTJ7gOjHYTikBAgAdPSNKSarUvYnBJLcfET26BBMSTA4uuVe-2FhlNZVWncg6B9q-2FXZKhAcCRCEKHv7GXcfS84Q7XO0VqQsrAirG2jP0OikPmyThOszeEK5egfCIkBqQxueJFMoJdzSh4Fr-2Fe9Tz0H7oJllnI7a3f54vCXMDvJCX-2B4gEd9P91GXO-2BGFV0UC00Al7ebpgPpfmwoymfqpkv-2F9-2BAcrXi4bqrVFhp8FUcPkEhu6xz1UC4Jdu-2FvYuEKuz1PtdBVpifFW6rhwl5EBd-2F08SkYPb4OS8WxHcID04HoX3aXjpr9Ac9QRN1uBaIT7qXLsNcpotZd-2FFVx-2BZGmRFdvoQvuLYRBPXAnAoHy-2BMBJnTvUgkJUeKGvLP1BjLaNxqz-2FATepCb97qNKGci3J-2BkJL3Cwq2FBjQ-3DCsQA_kuX2cI8baDAaOcj-2Fp3iIjU6R7PXKa3dAYAr0B7iMyKwz-2FaV0nnIuCVP1pcf8DEy1rdfWqpzAQNZA1Y921B94NpseWzcbMstrvSkpfFKcQtNXj3z-2FaLXLujj-2FpW85PHEzPXo7rxxM7mgjTDOWgLoMSIulyv-2Bc7fiA8Xb9BGOtJugXOqVoHxFOtJaLigWsu7pyJLcSvCDJRKADooFRKKz5r8eDbpFf5CzVjByKKPE0ih-2BfWHPHivxXDa4UZnNuWfLAqF1X3LQmY-2F5yLNAzoeL-2FBZe8hJTL5WzubEv0wwviiYGBFgtXAazwD1b7iJh7Mm5DfHVBxm-2FsYW0OzuYHlvRcb5QAtyBHghP5bNQUZRIxfYFBL4huimvcoAR85jEyNt1sak7dDkXaNmgvW58Bgo94W-2FaVgQ5o5PHLXkEeCebV0doseP-2Bb-2FFvSqV86R32I032TiZq8C2MeMrXaPYCjahl27oQf5PQUn5Saiya89RseMdY-3D"
        try:
            data = s.get(email_url,allow_redirects=False)
            cmpt_data = data.text.strip()
            print("Cmpt_data: "+str(cmpt_data))
            soup = BeautifulSoup(cmpt_data, 'html.parser')
            token_url = ""
            work_order_number = ""
            apm_vendor_id = ""
            vhost = ""
            for link in soup.findAll('a'):
                token_url = link.get('href')
                print("Token_url: "+str(token_url))
                logging.info("Token_url: "+str(token_url))


            #$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$Please change $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            # token_url = """https://vendor.appfolio.com/atlaspropertymanagementllc.appfolio.com/2231/workOrders/9593?idp_type=vendor&magic_link_token=eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOnsidmVyc2lvbiI6MSwiY29tcGFueV9lbWFpbCI6IndvcmtvcmRlcnNAbHVsYS5saWZlIiwidXNlcl9lbWFpbCI6IndvcmtvcmRlcnNAbHVsYS5saWZlIiwiY29tcGFueV9waG9uZV9udW1iZXIiOiIoOTEzKSAzMDMtNzc4MSIsInZob3N0X2d1aWQiOiIwMzZlMmM0OS1mNmZhLTQzOTUtYmZiNS0wYjhkYjIxMTA3NWQiLCJ1c2VyX3Nob3J0X3VpZCI6MjIzMSwiY29tcGFueV9uYW1lIjoiQXRsYXMgUHJvcGVydHkgTWFuYWdlbWVudCBMTEMifSwiZXhwIjoxNjg0NDcxMzcyLCJpc3MiOiJhdGxhc3Byb3BlcnR5bWFuYWdlbWVudGxsYy5hcHBmb2xpby5jb20ifQ.Lv65ByF5OXnQ_PNzQrLYs_2se_rpU6eys8F5x-16HFFnWAshuC7H5LF5t9RcwOK22EJc1Gv4_2McQabGjHDN_kkoTNlvP3VbCCGQ28M1VE6dFFbKU-YirSHShZ1_v9sMgnE1TZ8LlmejhV8yYXe0WY5dPDZAnbFNM_eljBUG2MrpgqC5npqb0FjZoEPecgx2aiF4QVDaNPPYnpfY_wNd2pKX_bQv8veD_m1iw3ON-ww-VJ3S9WvjH3BrpqPE0iqWh7iaNjEXqr5mKxujilFGAW5QNUqkgTD5G7oF3KTKnPOlgXiPGZg_y6csaVKZwpQ05fOrnvnD0CerngTS52hprA&ref=email"""
            # token_url = """https://vendor.appfolio.com/advantagehomes.appfolio.com/708/workOrders/2195?idp_type=vendor&magic_link_token=eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOnsidmVyc2lvbiI6MSwiY29tcGFueV9lbWFpbCI6IndvcmtvcmRlcnNAbHVsYS5saWZlIiwidXNlcl9lbWFpbCI6IndvcmtvcmRlcnNAbHVsYS5saWZlIiwiY29tcGFueV9waG9uZV9udW1iZXIiOiIoOTEzKSAzMDMtNzc4MSIsInZob3N0X2d1aWQiOiIwMzZlMmM0OS1mNmZhLTQzOTUtYmZiNS0wYjhkYjIxMTA3NWQiLCJ1c2VyX3Nob3J0X3VpZCI6MjIzMSwiY29tcGFueV9uYW1lIjoiQXRsYXMgUHJvcGVydHkgTWFuYWdlbWVudCBMTEMifSwiZXhwIjoxNjg0NDcxMzcyLCJpc3MiOiJhdGxhc3Byb3BlcnR5bWFuYWdlbWVudGxsYy5hcHBmb2xpby5jb20ifQ.Lv65ByF5OXnQ_PNzQrLYs_2se_rpU6eys8F5x-16HFFnWAshuC7H5LF5t9RcwOK22EJc1Gv4_2McQabGjHDN_kkoTNlvP3VbCCGQ28M1VE6dFFbKU-YirSHShZ1_v9sMgnE1TZ8LlmejhV8yYXe0WY5dPDZAnbFNM_eljBUG2MrpgqC5npqb0FjZoEPecgx2aiF4QVDaNPPYnpfY_wNd2pKX_bQv8veD_m1iw3ON-ww-VJ3S9WvjH3BrpqPE0iqWh7iaNjEXqr5mKxujilFGAW5QNUqkgTD5G7oF3KTKnPOlgXiPGZg_y6csaVKZwpQ05fOrnvnD0CerngTS52hprA&ref=email"""

            if token_url:
                if re.search("//appfol.io/", token_url):
                    data = s.get(token_url, allow_redirects=False)
                    token_url_data = data.text.strip()
                    print("token_url: " + str(token_url_data))
                    logging.info("token_url: " + str(token_url_data))
                    try:
                        token_url_json_data = json.loads(token_url_data)
                        if "original_url" in token_url_json_data.keys():
                            token_url = token_url_json_data["original_url"]
                            print("original_url"+str(token_url))
                            logging.info("original_url"+str(token_url))
                    except Exception as e:
                        print("Json validation error in search //appfol.io/ case "+str(e))
                        logging.info("Json validation error in search //appfol.io/ case: "+str(token_url_data)+" Error: "+str(e))
                        send_email_programmers("Json validation error in search //appfol.io/ case: "+str(token_url_data)+ "Error: "+str(e),"Appfolio: Json validation error in search //appfol.io/ case [Subject: "+str(subject))

                work_order_number_check = re.search(r'workOrders/(\d+)?', token_url)
                if work_order_number_check:
                    work_order_number = str(work_order_number_check.group(1))
                    print("work_order_number: "+str(work_order_number))
                    logging.info("work_order_number: "+str(work_order_number))
                else:
                    print("Unable to get work order id from token url: "+str(token_url))
                    logging.info("Unable to get work order id from token url: "+str(token_url))

                apm_vendor_id_check = re.search(r'/(\d+)/workOrders', token_url)
                if apm_vendor_id_check:
                    apm_vendor_id = str(apm_vendor_id_check.group(1))
                    print("apm_vendor_id: "+str(apm_vendor_id))
                    logging.info("apm_vendor_id: "+str(apm_vendor_id))
                else:
                    print("Unable to get apm_vendor_id from token url" + str(token_url))
                    logging.info("Unable to get apm_vendor_id from token url" + str(token_url))

                vhost_check = re.search(r'appfolio.com/(.*?)/', token_url)
                if vhost_check:
                    vhost = str(vhost_check.group(1))
                    print("vhost: "+str(vhost))
                    logging.info("vhost: "+str(vhost))
                else:
                    print("Unable to get vhost from token url" + str(token_url))
                    logging.info("Unable to get vhost from token url" + str(token_url))

                if work_order_number != "":
                    print("Got work_order_number: "+str(work_order_number))
                    logging.info("Got work_order_number: "+str(work_order_number))
                    if vhost != "":
                        print("Got vhost: " + str(vhost))
                        logging.info("Got vhost: " + str(vhost))
                        if apm_vendor_id != "":
                            print("Got apm_vendor_id: " + str(vhost))
                            logging.info("Got apm_vendor_id: " + str(vhost))
                            job_url = "https://vendor.appfolio.com/" + str(vhost) + "/" + str(
                                apm_vendor_id) + "/workOrders/" + str(work_order_number)
                            print("Job url: " + str(job_url))

                            # # ###### please change ################
                            # work_order_number = 22087984
                            # # ########## please change ############

                            ########################### Checking scrapper_jobs_error_logs table #######################################
                            cursor.execute(
                                "SELECT log_id FROM scrapper_jobs_error_logs WHERE job_reference_id=%s AND external_job_source_id=%s",
                                (work_order_number, external_job_source_id))
                            check_job_in_error_logs = cursor.fetchone()
                            print("Scrapper error job log table status: "+str(check_job_in_error_logs))
                            logging.info("Scrapper error job log table status: "+str(check_job_in_error_logs))
                            if check_job_in_error_logs:
                                print("Scrapper_job_error_logs table has job_reference_id: "+str(work_order_number))
                                logging.info('Workorder already exists in Scrapper_job_error_logs table: {}'.format(
                                    work_order_number))
                                send_email_programmers(
                                    "Job already present in scrapper error logs table job_reference_id: " + str(
                                        work_order_number),
                                    "Appfolio: Job already present in scrapper error logs table [Subject: " + str(
                                        subject) + " ]")
                                # cursor.execute(
                                #     "UPDATE scrapper_email_logs SET scraper_status=%s, status_message=%s WHERE scrapper_email_id=%s",
                                #     ("skipped", "job already present in scrapper error logs table", scrapper_email_id))
                                # print("Updated successfully")
                                return "Job already present in Scrapper error job log table"
                            else:
                                cursor.execute(
                                    "SELECT job_id FROM jobs LEFT JOIN external_job_source_work_orders USING (external_job_source_work_order_id) WHERE external_job_source_id = %s AND job_reference_id = %s LIMIT 1",
                                    (external_job_source_id, work_order_number))
                                check_job_exist = cursor.fetchone()
                                print("Jobs table status: "+str(check_job_exist))
                                logging.info("Jobs table status: "+str(check_job_exist))
                                if check_job_exist == None:
                                    # # ########### please change #############
                                    # work_order_number = 9534
                                    # # ########################################
                                    job_url = "https://vendor.appfolio.com/"+str(vhost)+"/"+str(apm_vendor_id)+"/workOrders/"+str(work_order_number)
                                    print("Job url: "+str(job_url))
                                    logging.info("Job url: "+str(job_url))
                                    magic_token = appfilio_login()
                                    if magic_token:
                                        print("Got magic token: "+str(magic_token))
                                        logging.info("Got magic token: "+str(magic_token))
                                        work_order_url = "https://vendor.appf.io/maintenance/api/work_orders/"+str(work_order_number)+".json?vhost="+str(vhost)+"&apm_vendor_id="+str(apm_vendor_id)
                                        print("Work order url: "+str(work_order_url))
                                        logging.info("Work order url: "+str(work_order_url))
                                        work_order_url_header = {
                                            'Accept': 'application/json',
                                            'Authorization' :"Bearer "+magic_token,
                                            'Content-Type' : 'application/json',
                                            'Referer' : 'https://vendor.appfolio.com/',
                                            'sec-ch-ua' : '" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90"',
                                            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
                                            'X-Requested-With' : 'XMLHttpRequest',
                                            'X-Vendor-Portal-Web-Client' : 'e274ab702e1ce5de7d7e2eba05e1f594784cd708'
                                        }
                                        try:
                                            work_order_details = s.get(work_order_url,headers=work_order_url_header).text
                                            print(work_order_details)
                                            if work_order_details.strip() != "":
                                                try:
                                                    work_order_details_json_data = json.loads(work_order_details)
                                                    # print(work_order_details_json_data)
                                                    if "workOrder" in work_order_details_json_data.keys():
                                                        work_order_details_data = work_order_details_json_data["workOrder"]
                                                        print(work_order_details_data)
                                                        portfolio_name = ""
                                                        if "portfolio_name" in work_order_details_data.keys():
                                                            portfolio_name = work_order_details_data["portfolio_name"]



                                                        print_data(work_order_details_data,job_url)
                                                        priority_datas = dict()
                                                        post_structured_datas(priority_datas, work_order_details_data, job_url,work_order_number)
                                                        post_json_data = json.dumps(priority_datas, indent=4)
                                                        print(post_json_data)
                                                        posted_response = ""
                                                        posted_json_response = ""

                                                        get_partner_id(apm_vendor_id,job_url,portfolio_name)
                                                        logging.info("partner_id data: "+str(partner_id))
                                                        print("Partner_id: "+str(partner_id))
                                                        if partner_id:
                                                            logging.info("Got partner_id: " + str(partner_id))
                                                            get_access_token(partner_id,job_url)

                                                            logging.info(
                                                                "Your appfolio_enabled: " + str(is_appfolio_enabled))
                                                            if is_appfolio_enabled == "yes":
                                                                print("Appfolio enable is not yes: " + str(job_url))
                                                                logging.info(
                                                                    "Skipping .. and this parnter is handled by appfolio api. " + str(
                                                                        job_url))
                                                                send_email_programmers("Skipping .. and this parnter is handled by appfolio api. "+str(job_url),"Appfolio: is_enabed is yes [Subject: " + str(
                                                                    subject))
                                                                no_for_display = ""
                                                                number_for_display = ""
                                                                if "numberForDisplay" in work_order_details_data.keys():
                                                                    number_for_display = work_order_details_data["numberForDisplay"]
                                                                    no_for_display = re.sub(r"\s+", "",number_for_display)
                                                                created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                                                cursor.execute("INSERT INTO scrapper_api_enabled_appfolio_jobs (number_for_display,job_reference_id,apm_vendor_id,vhost,url,created_at) VALUES (%s,%s,%s,%s,%s,%s)",(no_for_display,work_order_number,apm_vendor_id,vhost,str(job_url),created_at))
                                                                return "appfolio is_enable is yes"
                                                            else:
                                                                logging.info("api_access_token data: " + str(api_access_token))
                                                                if api_access_token:
                                                                    logging.info("Got api_access_token: " + str(api_access_token))
                                                                    try:
                                                                        headers = {
                                                                            "Content-Type": "application/json", "Accept": "application/json",
                                                                            "Authorization": "Bearer " + api_access_token
                                                                        }
                                                                        post_url = config['settings']['lula_post_url']
                                                                        p = s.post(post_url, data=post_json_data,
                                                                                   headers=headers)
                                                                        print(p.text)
                                                                        posted_response = p.text
                                                                        logging.info("Your posted_response: "+str(posted_response))
                                                                        posted_json_response = p.json()
                                                                        print(posted_json_response)
                                                                        if "message" in posted_json_response.keys():
                                                                            if posted_json_response["message"] != "Job added successfully.":
                                                                                logging.info(
                                                                                    "failed to add workorder-->message field not job success: " + str(
                                                                                        work_order_number))
                                                                                encoded_post_datas = posted_response + "\nJob_reference_no: " + str(
                                                                                    work_order_number) + "\nPost Data: " + post_json_data
                                                                                encoded_post_datas_programmers = posted_response + "\nJob_reference_no: " + str(
                                                                                    work_order_number) + "\nPost Data: " + post_json_data + "\nDatas: " + str(
                                                                                    work_order_details_data)
                                                                                subject_msg = "Appfolio: Failed to add job WO# " + str(
                                                                                    work_order_number) + " [Subject: " + str(subject) + "]"
                                                                                # cursor.execute(
                                                                                #     "UPDATE appfolio_email_logs SET scraper_status=%s, status_message=%s  WHERE scrapper_email_id=%s",
                                                                                #     ("error", str(posted_response), scrapper_email_id))
                                                                                insert_scrapper_job_error_logs(str(work_order_number), str(job_url), "unknown",
                                                                                                               "json_post_failed",
                                                                                                               "Failed to post the json data to partnerapi.lula-dev.com/partner/v1/jobs",
                                                                                                               str(work_order_details_data),
                                                                                                               str(posted_json_response),
                                                                                                               str(post_json_data),str(apm_vendor_id))
                                                                                send_email_programmers(encoded_post_datas_programmers, subject_msg)
                                                                                send_email_lula(encoded_post_datas, subject_msg)

                                                                                return "Failed to post the json data: message field not job success "+str(posted_response)
                                                                            else:
                                                                                encoded_post_datas = posted_response + "\nJob_reference_no: " + str(
                                                                                    work_order_number) + "\nPost Data: " + post_json_data
                                                                                print("New Job added successfully")
                                                                                logging.info("New Job added successfully with job_reference_no: " + str(
                                                                                    work_order_number))
                                                                                subject_msg = "Appfolio: New Job added sucessfully - WO# " + str(
                                                                                    work_order_number) + " [Subject: " + str(
                                                                                    subject) + "]"
                                                                                send_email_programmers(encoded_post_datas,
                                                                                                       subject_msg)

                                                                                ######################Accept workorder ######################
                                                                                if "accepted" in work_order_details_data.keys():
                                                                                    accepted = work_order_details_data[
                                                                                        "accepted"]
                                                                                    if accepted == 1:
                                                                                        print("Already accepted")
                                                                                        logging.info("Already accepted")
                                                                                    elif accepted == 0:
                                                                                        print("Not accepted")
                                                                                        logging.info("Not accepted")
                                                                                        print("Vhost: " + str(vhost))
                                                                                        logging.info("Accepted Vhost: " + str(vhost))
                                                                                        print("Apm_vendor_id:" + str(apm_vendor_id))
                                                                                        print("Work order number: " + str(work_order_number))
                                                                                        logging.info("Accepted Work order number: " + str(work_order_number))
                                                                                        accept_response = ""
                                                                                        try:
                                                                                            accept_response = s.post("https://vendor.appf.io/maintenance/api/work_orders/" + str(work_order_number) + "/accept",
                                                                                                params={"ref": "vendor_portal"},
                                                                                                json={"accept": "true",
                                                                                                      "vhost": str(vhost),
                                                                                                      "apm_vendor_id": str(apm_vendor_id)
                                                                                                      },
                                                                                                headers={
                                                                                                    "authorization": "Bearer " + str(magic_token),
                                                                                                    "x-requested-with": "XMLHttpRequest"
                                                                                                }
                                                                                            )

                                                                                            print(accept_response)
                                                                                            accepted_response_data = ""
                                                                                            accepted_response_json = ""
                                                                                            accepted_response_data = accept_response.text
                                                                                            print(accepted_response_data)
                                                                                            logging.info("accepted_response_data: " + str(accepted_response_data))
                                                                                            accepted_response_json = accept_response.json()
                                                                                            print(accepted_response_json)
                                                                                            if "success" in accepted_response_json.keys():
                                                                                                print("Acceptance success key found")
                                                                                                logging.info("Acceptance success key found")
                                                                                                if accepted_response_json["success"] == 1:
                                                                                                    print("Accepted sucessfully")
                                                                                                    logging.info("Accepted sucessfully")
                                                                                                    logging.info("Accepted sucessfully: " + str(accepted_response_json) + " work_order_no: " + str(work_order_number))
                                                                                                    send_email_programmers("Accepted sucessfully: " + str(accepted_response_json) + " work_order_no: " + str(work_order_number) + " job_url: " + str(job_url),"Appfolio: Accepted sucessfully success is True [Subject: " + str(subject))
                                                                                                else:
                                                                                                    print("Not accepted successfully")
                                                                                                    logging.info("Not accepted successfully: " + str(accepted_response_json) + " work_order_no: " + str(work_order_number))
                                                                                                    send_email_programmers("Not accepted successfully: " + str(accepted_response_json) + "work_order_no: " + str(work_order_number) + " job_url: " + str(job_url),"Appfolio: Not accepted successfully success is False [Subject: " + str(subject))
                                                                                            else:
                                                                                                print("No keyword success in accept response")
                                                                                                logging.info("No keyword success in accept response " + str(accepted_response_json))
                                                                                                send_email_programmers("No keyword success in accept response: " + str(accepted_response_json) + "job_url: " + str(job_url),"Appfolio: No keyword success in accept response [Subject: " + str(subject))
                                                                                        except Exception as e:
                                                                                            print("Something wrong in post request of accept response: " + str(e))
                                                                                            logging.info("Something wrong in post request of accept response: " + str(e))
                                                                                            send_email_programmers("Something wrong in post request of accept response: " + str(e) + "job_url: " + str(job_url),"Appfolio: Something wrong in post request [Subject: " + str(subject))
                                                                                else:
                                                                                    print("No keyword accepted in work order details json")
                                                                                    logging.info("No keyword accepted in work order details json")


                                                                                ############################################################################

                                                                                ############### Getting image datas ########################
                                                                                get_image_urls(work_order_details_data,
                                                                                               subject_msg,
                                                                                               work_order_number)
                                                                                return "New Job added successfully"
                                                                        else:
                                                                            print("No message found......")
                                                                            logging.info("No message found for job_reference_id: " + str(work_order_number))
                                                                            encoded_post_datas = posted_response + "\nJob_reference_no: " + str(
                                                                                work_order_number) + "\nPost Data: " + post_json_data
                                                                            encoded_post_datas_programmers = posted_response + "\nJob_reference_No: " + str(
                                                                                work_order_number) + "\nPost Data: " + post_json_data + "\nDatas: " + str(work_order_details_data)
                                                                            subject_msg = "Appfolio: Failed to add job WO# " + str(
                                                                                work_order_number) + " [Subject: " + str(
                                                                                subject) + "]"
                                                                            send_email_programmers(encoded_post_datas_programmers, subject_msg)
                                                                            send_email_lula(encoded_post_datas, subject_msg)
                                                                            # cursor.execute(
                                                                            #     "UPDATE scrapper_email_logs SET scraper_status=%s,status_message=%s WHERE scrapper_email_id=%s",
                                                                            #     ("error", str(posted_response), scrapper_email_id))
                                                                            insert_scrapper_job_error_logs(str(work_order_number), str(job_url), "unknown",
                                                                                                           "json_post_failed",
                                                                                                           "Failed to post the json data to partnerapi.lula-dev.com/partner/v1/jobs",
                                                                                                           str(work_order_details_data),
                                                                                                           str(posted_json_response),
                                                                                                           str(post_json_data),str(apm_vendor_id))
                                                                            return "Failed to post the json data: message field not found"

                                                                    except Exception as e:
                                                                        print("Unable to post data " + str(e))
                                                                        logging.info(
                                                                            "Unable to post data -- for job_reference_id: " + str(work_order_number))
                                                                        subject_msg = "Appfolio: [Subject: " + str(
                                                                            subject) + "] Failed to add job Work Order Number: " + str(
                                                                            work_order_number) + " unable to post"
                                                                        send_email_programmers(
                                                                            posted_response + "\nUnable to get response for WO#: " + str(
                                                                                work_order_number) + "\nPost Data: " + post_json_data + "Error: "+str(e),
                                                                            subject_msg)
                                                                        send_email_lula(posted_response + "\nUnable to get response for WO#: " + str(
                                                                            work_order_number) + "\nPost Data: " + post_json_data, subject_msg)
                                                                        logging.error('Unable to get response: {}'.format(e))
                                                                        print("Posted json resonse: "+str(posted_json_response))
                                                                        print("After inserting the exception")
                                                                        # cursor.execute(
                                                                        #     "UPDATE appfolio_email_logs SET scraper_status=%s, status_message=%s WHERE scrapper_email_id=%s",
                                                                        #     ("error", "unable to post data exception", scrapper_email_id))
                                                                        insert_scrapper_job_error_logs(str(work_order_number), str(job_url), "unknown",
                                                                                                       "json_post_failed",
                                                                                                       "Failed to post the json data to partnerapi.lula-dev.com/partner/v1/jobs",
                                                                                                       str(work_order_details_data), str(posted_json_response),
                                                                                                       str(post_json_data),str(apm_vendor_id))
                                                                        return "Failed to post the json data: something wrong"

                                                                else:
                                                                    print("Unable to get api_access_token for job_url: " + str(job_url))
                                                                    logging.info(
                                                                        "Unable to get api_access_token for job_url: " + str(job_url))
                                                                    send_email_programmers(
                                                                        "Unable to get api_access_token for job_url: " + str(job_url),
                                                                        "Appfolio: Failed to get api_access_token [Subject: " + str(
                                                                            subject))
                                                                    insert_scrapper_job_error_logs(str(work_order_number),
                                                                                                   str(job_url), "new",
                                                                                                   "missing_db_api_access_token",
                                                                                                   "api_access_token not found in partners table using partner_reference_id : " + str(
                                                                                                       apm_vendor_id) + " partner_id: " + str(
                                                                                                       partner_id),
                                                                                                   str(work_order_details_data),
                                                                                                   str(posted_json_response),
                                                                                                   str(post_json_data),
                                                                                                   str(apm_vendor_id))
                                                                    return "Failed to get api_access_token"

                                                        else:
                                                            print("Unable to get partner_id for job_url: "+str(job_url))
                                                            logging.info("Unable to get partner_id for job_url: "+str(job_url))
                                                            send_email_programmers(
                                                                "Unable to get partner_id for job_url: "+str(job_url),
                                                                "Appfolio: Failed to get partner_id [Subject: " + str(
                                                                    subject))

                                                            sendNewCustomerAccountAlert(apm_vendor_id,portfolio_name)
                                                            insert_scrapper_job_error_logs(str(work_order_number),
                                                                                           str(job_url), "new",
                                                                                           "missing_db_partner_id",
                                                                                           "partner_refefence_id/party_id not found in partners table using partner_reference_id : "+str(apm_vendor_id),
                                                                                           str(work_order_details_data),
                                                                                           str(posted_json_response),
                                                                                           str(post_json_data),
                                                                                           str(apm_vendor_id))
                                                            return "Unable to get partner_id"
                                                    else:
                                                        print("Unable to get data from work_order_details_json_data")
                                                        logging.info("Unable to get data from work_order_details_json_data no workOrder key: "+str(work_order_details_json_data))
                                                        send_email_programmers(
                                                            "Unable to get data from work_order_details_json_data no workOrder key: " + str(work_order_details_json_data),
                                                            "Appfolio: Unable to get data from work_order_details_json_data [Subject: " + str(
                                                                subject))
                                                        return "Unable to get data from work_order_details_json_data no workOrder key"

                                                except Exception as e:
                                                    print("Json validation issue at work_order_details_json_data: " + str(e))
                                                    logging.info("Json validation issue at work_order_details_json_data: " + str(e))
                                                    send_email_programmers(
                                                        "Json validation issue at work_order_details_json_data: " + str(e),
                                                        "Appfolio: Json validation issue at work_order_details_json_data [Subject: " + str(
                                                            subject))
                                                    return "Json validation issue at work_order_details_json_data"

                                            else:
                                                print("Unable to get work_order_details empty data: " + str(work_order_details))
                                                logging.info("Unable to get work_order_details empty data: " + str(work_order_url))
                                                send_email_programmers(
                                                    "Unable to get work_order_details empty data " + str(work_order_url),
                                                    "Appfolio: Failed to get work_order_details [Subject: " + str(
                                                        subject))
                                                return "Unable to get work_order_details empty data"

                                        except Exception as e:
                                            print("Unable to get response from work_order_url: "+str(e))
                                            logging.info("Unable to get response from work_order_url: "+str(e))
                                            send_email_programmers(
                                                "Unable to get response from work_order_url " + str(e),
                                                "Appfolio: Failed to get response from work_order_url [Subject: " + str(
                                                    subject))
                                            return "Unable to get response from work_order_url"

                                    else:
                                        print("Unable to get magic token from the login session")
                                        logging.info("Unable to get magic token from the login session")
                                        send_email_programmers(
                                            "Unable to get magic token from the login session " + str(token_url),
                                            "Appfolio: Failed to get magic token from the login session [Subject: " + str(
                                                subject))
                                        return "Unable to get magic token from the login session"
                                else:
                                    print("Jobs table already has job_reference_id: ", str(work_order_number))
                                    logging.info(
                                        "Job already present in jobs table for job_reference_id: " + str(work_order_number))
                                    send_email_programmers(
                                        "Job already present in jobs table job_reference_id: " + str(
                                            work_order_number),
                                        "Appfolio: Job already present in jobs table [Subject: " + str(
                                            subject) + " ]")
                                    # cursor.execute(
                                    #     "UPDATE appfolio_email_logs SET scraper_status=%s, status_message=%s WHERE scrapper_email_id=%s",
                                    #     ("skipped", "job already present in jobs table", scrapper_email_id))
                                    logging.info('Workorder already exists in jobs table{}'.format(work_order_number))
                                    return "Job already present in jobs table"

                        else:
                            print("Unable to get details apm_vendor_id from the token url")
                            logging.info("Unable to get details apm_vendor_id from the token url: " + str(token_url))
                            send_email_programmers(
                                "Unable to get details apm_vendor_id from the token url: " + str(token_url),
                                "Appfolio: Failed to get apm_vendor_id from the token url [Subject: " + str(
                                    subject))
                            return "Unable to get details apm_vendor_id from the token url"

                    else:
                        print("Unable to get details vhost from the token url")
                        logging.info("Unable to get details vhost from the token url: " + str(token_url))
                        send_email_programmers(
                            "Unable to get details vhost from the token url: " + str(token_url),
                            "Appfolio: Failed to get vhost from the token url [Subject: " + str(
                                subject))
                        return "Unable to get details vhost from the token url"
                else:
                    print("Unable to get details work_order_number from the token url")
                    logging.info("Unable to get details work_order_number from the token url: "+str(token_url))
                    send_email_programmers("Unable to get details work_order_number from the token url: " + str(token_url),
                                           "Appfolio: Failed to get work_order_number from the token url [Subject: " + str(
                                               subject))
                    return "Unable to get work_order_number from the token url"
            else:
                print("Unable to get token url from: "+str(cmpt_data))
                logging.info("Unable to get token url from: " + str(cmpt_data))
                send_email_programmers("Unable to get token url from: " + str(cmpt_data),
                                       "Appfolio: Failed to get work_order_number from the token url [Subject: " + str(
                                           subject))
                return "Unable to get token url"

        except Exception as e:
            print("Unable to get response from the html url data: "+str(e))
            logging.info("Unable to get response from the html url data: "+str(e))
            send_email_programmers("Unable to get response from the html url data: "+str(e),"Appfolio: Failed to get response from the html url data [Subject: "+str(subject))
            return "Unable to get response from the html url data"
    else:
        print("Unable to get the url from the email html content")
        logging.info("Unable to get the url from the email html content: "+str(email_html_content))
        send_email_programmers("Unable to get the url from the email html content: " + str(email_html_content),
                               "Appfolio: Failed to get the url from the email html content [Subject: " + str(subject))
        return "Unable to get the url from the email html content"



def get_appfolio_datas():
    email_content = ""
    message_id = ""
    from_email_address = ""
    email_html_content = ""
    subject = ""
    to_address = ""
    try:
        arguments = cgi.FieldStorage()
        for key in arguments.keys():

            if key == "body-html":
                email_html_content = arguments[key].value
            else:
                logging.info("Data: key: " + str(key) + "value: " + str(arguments[key].value))
                data = "%s=>%s" % (key, arguments[key].value)
                print(data)
                email_content = email_content + "\n" + data
                logging.info("The email content: " + str(email_content))
                if key == "Message-Id":
                    message_id = arguments[key].value
                    print("Your message id: " + str(message_id))
                    logging.info("Your message id: " + str(message_id))
                elif key == "From":
                    from_add = arguments[key].value
                    from_name, from_email_address = parseaddr(from_add)
                    print("Email from address: " + str(from_email_address))
                    logging.info("Email from address: " + str(from_email_address))
                elif key == "Subject":
                    subject = arguments[key].value
                    print("Subject: " + str(subject))
                    logging.info("Subject: " + str(subject))
                elif key == "To":
                    to_address = arguments[key].value
                    print("To address: " + str(to_address))
                    logging.info("To address: " + str(to_address))

        if email_content == "":
            print("No data present")
            logging.info("No data present")
            print("No data found")
        else:
            if message_id:
                print("Message_id exist: " + str(message_id))
                logging.info("Message_id exist: " + str(message_id))
                create_file(message_id, email_content)
            else:
                print("No message_id not found")
                logging.info("No message_id not found to create file")
            try:
                logging.info("Going to insert.. email_content: " + str(email_content))
                cursor.execute(
                    "INSERT INTO appfolio_email_logs (message_id,email_subject,from_address,to_address,email_html_content,email_content) VALUES (%s,%s,%s,%s,%s,%s)",
                    (message_id, subject, from_email_address, to_address, email_html_content, str(email_content)))
                scrapper_email_id = cursor.lastrowid
                print("scrapper_email_id: " + str(scrapper_email_id))
                logging.info("insert_scrapper_email_logs the lastrowid: " + str(scrapper_email_id))
                print("Inserted successfully")
                print("Getting data from body html: " + str(email_html_content))

                ######################Checking From address######################################

                if re.search("appfolio.us$", from_email_address):
                    print("Valid from address: " + str(from_email_address))
                    logging.info("Valid from address: " + str(from_email_address))
                else:
                    print("Not a valid from address: " + str(from_email_address))
                    logging.info("Not a valid from address: " + str(from_email_address))
                    send_email_programmers(str(email_content),
                                           "Appfolio: Not a valid from address")
                    print("Appfolio: Not a valid email from address")
                    return "Appfolio: Not a valid email from address"

                ######################Checking From address######################################

                ################### Check view work order in email html content ##########################
                soup = BeautifulSoup(email_html_content, 'html.parser')
                url = soup.find("a", text=re.compile("view work order|view this work order", re.IGNORECASE))
                print(url)
                logging.info("Your url data: " + str(url))
                if url:
                    print("Got url with view work order key in email_html_content")
                    logging.info("Got url with view work order key in email_html_content: " + str(url))
                else:
                    print("Not a valid email html content")
                    logging.info("Not a valid email html content: " + str(email_html_content))
                    send_email_programmers(str(email_content), "Appfolio: Not a valid email html content")
                    print("Not a valid email html content")
                    return "Not a valid email html content"
                ###################Check view work order in email html content################################

                process_status = ""
                process_status = get_data(email_html_content, subject)
                if process_status == "New Job added successfully":
                    print("New Job added successfully")
                    return "New Job added successfully"
                else:
                    print(process_status)
                    return process_status

            except Exception as e:
                logging.info("Unable to insert into the table email content: " + str(email_content) + " error:" + str(e))
                print("Unable to insert into the table : " + str(e))
                send_email_programmers("Failed to insert into table" + str(e),
                                       "Appfolio: Failed to insert into the table appfolio_email_logs subject: " + str(
                                           subject))
                print("Failed to insert into the appfolio_email_logs table data")
                return "Failed to insert into the appfolio_email_logs table data"

    except Exception as e:
        print("Unable to get key values from request post")
        print(e)
        send_email_programmers(str(e), "Appfolio: Failed to get key values from request post")
        logging.info("Unable to get key values from request post: " + str(e))
        print("Unable to get key values from request post")
        return "Unable to get key values from request post"


start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(start_time)

logging.basicConfig(filename=log_file(), level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

try:
    db = pymysql.connect(host=config['mysqlDB']['host'], user=config['mysqlDB']['user'],
                         password=config['mysqlDB']['password'], database=config['mysqlDB']['database'],
                         port=int(config['mysqlDB']['port']), autocommit=True)
    cursor = db.cursor(pymysql.cursors.DictCursor)
    print("Database connected sucessfully")
except pymysql.err.OperationalError as e:
    print("Database Connection Failure " + str(e))
    subject_msg = "Database Connection Failure"
    send_email_programmers("Unable to connect database " + str(e), subject_msg)

get_appfolio_datas()


