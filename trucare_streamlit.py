import streamlit as st
import pandas as pd
import datetime
import hashlib
import json
import os
import uuid
import time
import json
import sys
import tempfile

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
from azure.core.exceptions import ResourceExistsError
from awesome_table import AwesomeTable

from pathlib import Path
from pdf2image import convert_from_path
import base64

def show_pdf(file_path:str):
    """Show the PDF in Streamlit
    That returns as html component

    Parameters
    ----------
    file_path : [str]
        Uploaded PDF file path
    """
    with open(file_path, "rb") as f:
        base64_pdf = base64.b64encode(f.read()).decode("utf-8")
    pdf_display = f'<embed src="data:application/pdf;base64,{base64_pdf}" width="100%" height="1000" type="application/pdf">'
    st.markdown(pdf_display, unsafe_allow_html=True)

CONNECTION_STR = "Endpoint=sb://trumarine.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Evwzn/+LuJU3Ef6FSG8g7/6HLmq37g4YJtC26hNXqFs="
#Endpoint=sb://trumarine.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Evwzn/+LuJU3Ef6FSG8g7/6HLmq37g4YJtC26hNXqFs=
# QUEUE_NAME_1 = "crew-service-form"
QUEUE_NAME_2 = "mro-service-form"
QUEUE_NAME_3 = "turbo-session" #message bus receiver (session id) from blockchain
QUEUE_NAME_4 = "turbo-query"   #message bus sender for turbocharger status w/o session id to blockchain
#QUEUE_NAME_5 = "part-query"    #message bus sender for spare part status w/o session id to blockchain
#SESSION_SEND = "U-001"
SESSION_RECV = "V-"
BLOB_CONNSTRING = "DefaultEndpointsProtocol=https;AccountName=trumarine;AccountKey=wrK3sjyeCPqFUhGd+xVOv98XmtO9hFudqkcttpjQ9+5fGuWKa9os2hhke/h6EoY84MC2LxEUcIYb+khTwJgsfA==;EndpointSuffix=core.windows.net"

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)
st.set_page_config(layout="wide")
st.title('TruCare Blockchain')

#css to inject contained in a string
hide_table_row_index = """
        <style>
        thead tr th:first-child {display:none}
        tbody th {display:none}
        </style>
        """

def overview_page():
    #with st.form(key="my_form"):
    out_json = dict()
    out_json['Command'] = 'query'
    out_json['CommandID'] = uuid.uuid4().hex
    out_json['Class_Type'] = 'Vessel'
    with servicebus_client:
            # get a Queue Sender object to send messages to the queue
        sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME_4)
        with sender:
            msg = ServiceBusMessage(str(out_json))
            sender.send_messages(msg)
        recv_session = SESSION_RECV + out_json['CommandID'] 
            #st.write(recv_session)
        result = []
        receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME_3, session_id=recv_session)   
        with receiver:
            session = receiver.session
            session.set_state("START")
            received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=15) #, max_wait_time=5
            for msg in received_msgs:
                result.append(str(msg))
                    #st.write(msg)
                receiver.complete_message(msg)
                session.renew_lock()
            session.set_state("END")
        #st.write(result)
        result_str = str(result[0])
        result_str = result_str.replace("\'", "\"")
        result_json = json.loads(result_str)
        make_list = list(result_json.keys())
        for j in range(len(make_list)-1):
            with st.container():
                st.subheader(make_list[j])
                pd_json = pd.json_normalize(result_json[make_list[j]])
                id = 0
                df_make = list()
                for k, v in pd_json.items():
                    if v[0]:
                        df_dict = dict()
                        df_dict['id'] = id + 1
                        df_dict['key'] = k #s[s.find("(")+1:s.find(")")]
                        df_dict['value'] = v.values[0]
                        df_make.append(df_dict)
                        id += 1
                new_tab = pd.json_normalize(df_make)
                grid_options = {
                    "columnDefs": [ {
                        "headerName": "ID",
                        "field": "id",
                        "width": 100
                    },
                    {
                        "headerName": "KEY",
                        "field": "key",
                        "width": 350
                    },
                    {
                        "headerName": "VALUE",
                        "field": "value",
                        "width": 850
                    }
                    ]
                }
                AgGrid(new_tab, grid_options) 
                #builder = GridOptionsBuilder.from_dataframe(df)
                #builder.configure_column("first_column", header_name="First", editable=True)
                #go = builder.build()
                #AgGrid(new_tab)

def subquery_form():
    with st.form(key='my_form'):
        out_json = dict() 
        turbo_id = st.selectbox('Turbocharger ID', turbo_tid)
        part_option = st.selectbox('Part', parts_name)
        part_number = st.text_input(label='Serial Number [optional]')
        out_json['Class_Type'] = part_option
        out_json['Turbocharger_ID'] = turbo_id
        if (part_number):
            out_json['Serial_Number'] = part_number
        out_json['Command'] = 'query'
        out_json['CommandID'] = uuid.uuid4().hex
        submitted = st.form_submit_button(label='Submit')
        if submitted:
            #key = out_json['Class_Type'].upper() + str(out_json['Turbocharger_ID'])
            result = []
            #st.write(out_json)
            with servicebus_client:
                # get a Queue Sender object to send messages to the queue
                sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME_4)
                with sender:
                    msg = ServiceBusMessage(str(out_json))
                    sender.send_messages(msg)
                # get a unique RECV_SESSION_ID
                # time.sleep(10)
                recv_session = SESSION_RECV + out_json['CommandID'] 
                #st.write(recv_session)
                receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME_3, session_id=recv_session)   
                with receiver:
                    session = receiver.session
                    session.set_state("START")
                    received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=5)
                    for msg in received_msgs:
                        result.append(msg)
                        #print(msg)
                        #st.write(str(msg))
                        receiver.complete_message(msg)
                        session.renew_lock()
                    session.set_state("END")
            #st.write(str(result[0]))
            result_str = str(result[0])
            result_str = result_str.replace("\'", "\"")
            result_json = json.loads(result_str)
            pd_json = pd.json_normalize(result_json)
            id = 0
            df_make = list()
            for k, v in pd_json.items():
                if v[0]:
                    df_dict = dict()
                    df_dict['id'] = id + 1
                    df_dict['key'] = k[k.find(".")+1:]
                    df_dict['value'] = v.values[0]
                    df_make.append(df_dict)
                    id += 1
            new_tab = pd.json_normalize(df_make)
            grid_options = {
                    "columnDefs": [ {
                        "headerName": "ID",
                        "field": "id",
                        "width": 100
                    },
                    {
                        "headerName": "KEY",
                        "field": "key",
                        "width": 350
                    },
                    {
                        "headerName": "VALUE",
                        "field": "value",
                        "width": 850
                    }
                    ]
                }
            AgGrid(new_tab, grid_options) 

def query_form():
    with st.form(key='my_form'):
        out_json = dict()
        class_option = st.selectbox('Class', class_name)
        turbo_id = st.selectbox('ID', turbo_tid)
        out_json['Class_Type'] = class_option
        out_json['Id'] = turbo_id
        out_json['Command'] = 'query'
        out_json['CommandID'] = uuid.uuid4().hex
        # get a unique SEND_SESSION_ID
        #out_json['SessionID'] = SESSION_SEND
        submitted = st.form_submit_button(label='Submit')
        if submitted:
            #+st.write(out_json)
            key = out_json['Class_Type'].upper() + str(out_json['Id'])
            result = []
            with servicebus_client:
                # get a Queue Sender object to send messages to the queue
                sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME_4)
                with sender:
                    msg = ServiceBusMessage(str(out_json))
                    sender.send_messages(msg)
                # get a unique RECV_SESSION_ID
                recv_session = SESSION_RECV + out_json['CommandID']  
                receiver = servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME_3, session_id=recv_session)
                with receiver:
                    session = receiver.session
                    session.set_state("START")
                    received_msgs = receiver.receive_messages(max_message_count=10, max_wait_time=15)
                    for msg in received_msgs:
                        result.append(msg)
                        #st.write(key)
                        #st.write(str(msg))
                        #st.write(str(msg[key]))
                        receiver.complete_message(msg)
                        session.renew_lock()
                    session.set_state("END")
            result_str = str(result[0])
            result_str = result_str.replace("\'", "\"")
            result_json = json.loads(result_str)
            pd_json = pd.json_normalize(result_json[key])
            id = 0
            df_make = list()
            for k, v in pd_json.items():
                if v[0]:
                    df_dict = dict()
                    df_dict['id'] = id + 1
                    df_dict['key'] = k
                    df_dict['value'] = v.values[0]
                    df_make.append(df_dict)
                    id += 1
            new_tab = pd.json_normalize(df_make)
            grid_options = {
                    "columnDefs": [ {
                        "headerName": "ID",
                        "field": "id",
                        "width": 100
                    },
                    {
                        "headerName": "KEY",
                        "field": "key",
                        "width": 350
                    },
                    {
                        "headerName": "VALUE",
                        "field": "value",
                        "width": 850
                    }
                    ]
                }
            AgGrid(new_tab, grid_options)

def crew_submit_form():
    df = pd.DataFrame('', index=range(10), columns=['part_types','part_num'])
    dropdownlist = ('Compressor wheel', 'Turbine rotor, blades and shaft', 'Nozzle ring', 'Diffuser', 'Bearing housing', 'Compressor housing', 'Turbine housing')
    grid_options = {
        "columnDefs": [
            {
                "headerName": "Spare Part Types",
                "field": "types",
                "editable": True,
                "width": 500,
                "cellEditor": "agSelectCellEditor",
                "cellEditorParams": {"values": dropdownlist}
            },
            {
                "headerName": "Spare Part S/Ns",
                "field": "sn",
                "editable": True,
                "width": 500
            },
        ],
        "rowData": [
            {'types': 'Compressor wheel', 'sn': 'abc123'}
        ],
        "defaultColDef": {
            "editable": True,
            "sortable": True,
            "flex": 1,
            "minWidth": 500,
            "filter": True,
            "resizable": True
        }
    }
    
    with st.form(key='crew_form', clear_on_submit=True):
        #vessel_id = st.text_input(label='Vessel ID *') #read from vessel_list api
        out_json = dict()
        c1, c2 = st.columns(2)
        with c1:
            vessel_option = st.selectbox('Vessel *', vessel_name)
        with c2:
            component = st.text_input(label='Component S/N', placeholder='SN706191')   
        #fname = st.text_input(label='First name')
        #lname = st.text_input(label='Last name')
        col1, col2 = st.columns(2)
        with col1:
            fname = st.text_input(label='First name', placeholder='John')
        with col2:
            lname = st.text_input(label='Last name', placeholder='Doe')
        col3, col4 = st.columns(2)
        with col3:
            record = st.text_input(label='Crew Service Record ID', placeholder='E1637')
        with col4:
            job = st.text_input(label='Job ID', placeholder='NA357')
        jobdescription = st.text_area(label='Job Description', placeholder='write the description here')
        col5, col6 = st.columns(2)
        with col5:
            datedone = st.date_input('Date of Service *', today)
        #datedone = st.text_input(label='Data Done (dd/dd/yyyy) *') #get date chart button
        #status = st.text_input(label='Status *') #drop down
        with col6:
            status = st.selectbox('Maintenance Status *', ('New', 'Refurbished', 'Decommisioned'))
        col7, col8 = st.columns(2)
        with col7:
            cost = st.number_input(label='Actual Cost (USD)', value=0)
        with col8:
            location = st.text_input(label='Service Location', placeholder='Lincoln')
        hours = st.number_input(label='Running Hours', value=0)
        grid_return = AgGrid(df, grid_options, editable = True, fit_columns_on_grid_load=True) 
        selected_rows = grid_return['data']
        out_json['Class_Type'] = 'CREW_SERVICE_RECORD'
        out_json['Vessel_ID'] = vessel_option[vessel_option.find("(")+1:vessel_option.find(")")] #s[s.find("(")+1:s.find(")")]
        if component:
            out_json['Component_SN'] = component
        if record:
            out_json['Service_Record_ID'] = record
        if fname:
            out_json['Name_of_Staff_Responsible'] = fname + ' ' + lname
        if job:
            out_json['Job_ID'] = job
        if jobdescription:
            out_json['Job_Description'] = jobdescription
        if location:
            out_json['Service_Location'] = location
        out_json['Date_Done'] = datedone.strftime('%Y-%m-%d')
        out_json['Maintenance_Status'] = status
        if float(cost) > 0:
            out_json['Actual_Cost'] = float(cost)
        if int(hours) > 0:
            out_json['Running_Hours'] = int(hours)
        uploaded_file = st.file_uploader("Upload your file")
        submitted = st.form_submit_button(label='Submit')
        if submitted:  
            if 'Service_Record_ID' in out_json.keys():
                selected_rows = grid_return['data'].dropna()
                #st.write("hi", selected_rows.loc[0, 'part_types'])
                if (selected_rows.loc[0, 'part_types']):
                    out_json['List_of_Spare_Parts_Types_Used'] = selected_rows['types'].values.tolist()
                    out_json['List_of_Spare_Parts_Used'] = selected_rows['sn'].values.tolist()
                if uploaded_file is not None:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        st.markdown("## Original PDF file")
                        fp = Path(tmp_file.name)
                        fp.write_bytes(uploaded_file.getvalue())
                        st.write(show_pdf(tmp_file.name))
                    #for file in uploaded_file:
                        presentDate = int(time.time())
                        out_json['Report_Version'] = presentDate
                        filename = uploaded_file.name
                        splitname = filename.split(".")
                        printname = ''
                        for p in range(len(splitname)-1):
                            printname += splitname[p]
                        printname += str(presentDate) 
                        printname += "." + splitname[-1]
                        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNSTRING)
                        container_client = blob_service_client.get_container_client("wp6fileupload")
                        try:
                            container_client.create_container()
                        except ResourceExistsError:
                            pass
                        
                        bytes = tmp_file.read()
                        myHash = hashlib.sha256(bytes).hexdigest()
                        #with open(uploaded_file.name, "rb") as data:
                        blob_client = container_client.upload_blob(name=printname, data=uploaded_file.read())
                        bloburl = "https://trumarine.blob.core.windows.net/wp6fileupload/" + printname
                        st.write(bloburl)
                        out_json['Report_URL'] = bloburl
                        out_json['Hash_Signature_Of_Report'] = myHash
                with servicebus_client:
                    # get a Queue Sender object to send messages to the queue
                    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME_2)
                    with sender:
                        st.write(out_json)
                        msg = ServiceBusMessage(str(out_json))
                        sender.send_messages(msg)
                st.write('Record is successfully uploaded')
            else:
                st.write('Please fill in the crew service record ID!')
                #st.write('Please upload your file!')
                #st.write(grid_return)

def submit_form(): 
    df = pd.DataFrame('', index=range(10), columns=['part_types','part_num'])
    dropdownlist = ('Compressor wheel', 'Turbine rotor, blades and shaft', 'Nozzle ring', 'Diffuser', 'Bearing housing', 'Compressor housing', 'Turbine housing')
    grid_options = {
        "columnDefs": [
            {
                "headerName": "Spare Part Types",
                "field": "types",
                "editable": True,
                "width": 500,
                "cellEditor": "agSelectCellEditor",
                "cellEditorParams": {"values": dropdownlist}
            },
            {
                "headerName": "Spare Part S/Ns",
                "field": "sn",
                "editable": True,
                "width": 500
            },
        ],
        "rowData": [
            {'types': 'Compressor wheel', 'sn': 'abc123'}
        ],
        "defaultColDef": {
            "editable": True,
            "sortable": True,
            "flex": 1,
            "minWidth": 500,
            "filter": True,
            "resizable": True
        }
    } 
    with st.form(key='my_form', clear_on_submit=True):
        out_json = dict()
        c1, c2 = st.columns(2)
        with c1:
            uploaded_fname = st.text_input(label='First name', placeholder='Justin')
        with c2:
            uploaded_lname = st.text_input(label='Last name', placeholder='Lin')
        c3, c4 = st.columns(2)
        with c3:
            uploaded_cname = st.text_input(label='Company *', placeholder='Trumarine')
        with c4:
            uploaded_ename = st.text_input(label='E-mail address', placeholder='justinlin@trumarine.com')
        c5, c6 = st.columns(2)
        with c5:
            vessel_option = st.selectbox('Vessel *', vessel_name)
        with c6:
            turbo_option = st.selectbox('Turbocharger *', turbo_name)
        c7, c8 = st.columns(2)
        with c7:
            uploaded_cust = st.text_input(label='Customer name *', placeholder='Teh Koon Jin')
        with c8:
            uploaded_cmail = st.text_input(label='Customer email address', placeholder='unknown')
        c9, c10 = st.columns(2)
        with c9:
            uploaded_po = st.text_input(label='Cust. PO number *', placeholder='MNS-202200107-1')
        with c10:
            uploaded_rfq = st.text_input(label='RFQ number', placeholder='unknown')
        c11, c12 = st.columns(2)
        with c11:
            uploaded_sid = st.text_input(label='Service record ID', placeholder='SO102223737')
        with c12:
            service_option = st.selectbox('Reason for Service *', ('Planned Inspection', 'Warranty', 'Damages', 'Others'))
        c13, c14 = st.columns(2)
        with c13:
            uploaded_sloc = st.text_input(label='Service location', placeholder='PSA, Singapore')
        with c14:
            uploaded_date = st.date_input('Date of service *', today)
        hours = st.number_input(label='Running hours', value=10000)
        grid_return = AgGrid(df, grid_options, editable = True, fit_columns_on_grid_load=True) 
        out_json['Class_Type'] = 'SERVICE_RECORD'
        out_json['Vessel_ID'] = vessel_option[vessel_option.find("(")+1:vessel_option.find(")")]
        out_json['Turbocharger_ID'] = turbo_option
        if uploaded_fname:
            out_json['Service_Provider-First_Name'] = uploaded_fname
        if uploaded_lname:
            out_json['Service_Provider-Last_Name'] = uploaded_lname
        if uploaded_cname:    
            out_json['Service_Provider-Company'] = uploaded_cname
        if uploaded_ename:
            out_json['Service_Provider-Email_Address'] = uploaded_ename
        if uploaded_cust:
            out_json['Customer_Name'] = uploaded_cust
        if uploaded_cmail:
            out_json['Customer_Email_Address'] = uploaded_cmail
        out_json['Date_Of_Service'] = uploaded_date.strftime('%Y-%m-%d')
        if uploaded_po:
            out_json['PO_Number'] = uploaded_po
        if uploaded_rfq:
            out_json['RFQ_Number'] = uploaded_rfq
        if uploaded_sid:
            out_json['Service_Record_ID'] = uploaded_sid
        if service_option:
            out_json['Reason_For_Service'] = service_option
        if uploaded_sloc:
            out_json['Service_Location'] = uploaded_sloc
        if hours:
            out_json['Running_Hours'] = hours
        uploaded_file = st.file_uploader("Upload your file")
        submitted = st.form_submit_button(label='Submit')
        if submitted:
            if 'Service_Record_ID' in out_json.keys():
                selected_rows = grid_return['data'].dropna()
                if (selected_rows.loc[0, 'part_types']):
                    out_json['List_of_Spare_Parts_Types_Used'] = selected_rows['types'].values.tolist()
                    out_json['List_of_Spare_Parts_Used'] = selected_rows['sn'].values.tolist()
                if uploaded_file is not None:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        st.markdown("## Original PDF file")
                        fp = Path(tmp_file.name)
                        fp.write_bytes(uploaded_file.getvalue())
                        st.write(show_pdf(tmp_file.name))
                        presentDate = int(time.time())
                        out_json['Report_Version'] = presentDate
                        filename = uploaded_file.name
                        splitname = filename.split(".")
                        printname = ''
                        for p in range(len(splitname)-1):
                            printname += splitname[p]
                        printname += str(presentDate) 
                        printname += "." + splitname[-1]
                        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNSTRING)
                        container_client = blob_service_client.get_container_client("wp6fileupload")
                        try:
                            container_client.create_container()
                        except ResourceExistsError:
                            pass
                        
                        bytes = tmp_file.read()
                        myHash = hashlib.sha256(bytes).hexdigest()
                        #with open(uploaded_file.name, "rb") as data:
                        blob_client = container_client.upload_blob(name=printname, data=uploaded_file.read())
                        bloburl = "https://trumarine.blob.core.windows.net/wp6fileupload/" + printname
                        st.write(bloburl)
                        out_json['Full_Report_URL'] = bloburl
                        out_json['Hash_Signature_Of_Full_Report'] = myHash
                with servicebus_client:
                    # get a Queue Sender object to send messages to the queue
                    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME_2)
                    with sender:
                        st.write(out_json)
                        msg = ServiceBusMessage(str(out_json))
                        sender.send_messages(msg)
                st.write('Record is successfully uploaded')
            else:
                st.write('Please fill in the MRO service record ID!')
            #st.write('Please upload your file!')
                #st.write(grid_return)
            

pages = ['Overview','MRO Service Report','Crew Service Report','Turbocharger Service Record', 'Spare Part Service Record']

vessel_name = ('Kota Manis (9632741)', 'Kota Makmur (9632753)', 'Kota Ganding (9626431)', 'Kota Gabung (9616852)')
turbo_name = ('ST5-148/12 (No.4)-7500010', 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 41, 42, 43, 44, 45, 46)
class_name = ('Turbocharger',)
parts_name = ('Compressor_Wheel', 'TURBINE_ROTOR_BLADES_SHAFT', 'NOZZLE_RING', 'DIFFUSER', 'BEARING_HOUSING', 'COMPRESSOR_HOUSING', 'TURBINE_HOUSING',)
turbo_tid = ('ST5-148/12 (No.4)-7500010', 21, 22, 23, 24, 25, 26, 31, 32, 33, 34, 35, 36, 41, 42, 43, 44, 45, 46)

spareparts = ['Compressor wheel', 'Turbine rotor, blades and shaft', 'Nozzle ring', 'Diffuser', 'Bearing housing', 'Compressor housing', 'Turbine housing']
spareparts_selected = ['Compressor wheel', 'Turbine rotor, blades and shaft']

today = datetime.date.today()

option = st.sidebar.selectbox(
    '',
    (pages)
)

if option == 'Overview':
    st.header(option)
    overview_page()

if option == 'MRO Service Report':
    st.header(option)
    submit_form()

if option == 'Crew Service Report':
    st.header(option)
    crew_submit_form()

if option == 'Turbocharger Service Record':
    st.header(option)
    query_form()

if option == "Spare Part Service Record":
    st.header(option)
    subquery_form()