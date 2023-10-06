import imap_tools as imap
import sqlite3 as sql
import os
from dotenv import load_dotenv
conn = sql.connect('message.db')
cursor = conn.cursor()

load_dotenv()

def getMaxOfUIDs(mailbox: imap.mailbox.MailBox, folder: str) -> str:
    mailbox.folder.set(folder)
    for msg in mailbox.fetch('ALL'):
            first_subjs = int(msg.uid) 
            for msgs in mailbox.fetch('ALL', reverse=True):
                sec_subjs = int(msgs.uid)
                return str(sec_subjs - first_subjs)
            

def get_max_id_of_table(table_name: str) -> str | None:

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = cursor.fetchone()

    if table_exists:
       
        cursor.execute(f"SELECT MAX(id) FROM {table_name}")
        max_id = cursor.fetchone()[0]
        
        return str(max_id)
    else:

        
        return None
def create_table_if_not_exist(folder: str) -> None:
    cursor.execute(f'''
                                CREATE TABLE IF NOT EXISTS '{''.join(e for e in folder if e.isalnum())}' (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    header TEXT,
                                    date TEXT,
                                    sender TEXT,
                                    reply_to TEXT,
                                    recipient TEXT,
                                    uid TEXT,
                                    text TEXT,
                                    html TEXT
                                )
                            ''')
    return None
def insert_message_data_to_db(message_data: dict, folder: str) -> None:
    cursor.execute(f'''
                                            INSERT INTO {''.join(e for e in folder if e.isalnum())} (header, date, sender, recipient, uid, text, html)
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                    ''',(message_data['header'], message_data['date'], message_data['sender'],
                                            message_data['recipient'][0], message_data['uid'], message_data['text'], message_data['html']))
    conn.commit()

def generate_message_data(msg: imap.message.MailMessage) -> dict:
    return {
                                            'header': msg.subject,
                                            'date': msg.date_str,
                                            'sender': msg.from_,
                                            'reply_to': msg.reply_to,
                                            'recipient': msg.to,
                                            'uid': msg.uid,
                                            'text': msg.text,
                                            'html': msg.html
                                    }


def start():
    
    program_ends = False
    while not program_ends:
        try:
            with imap.MailBox(os.environ['DOMAIN']).login(os.environ['LOGIN'], os.environ['PASSWORD']) as mailbox:
                for folder in mailbox.folder.list():
                    max_id_of_exists_table = get_max_id_of_table(''.join(e for e in folder.name if e.isalnum()))
                    skip_current_folder = False
                    max_uid = getMaxOfUIDs(mailbox=mailbox, folder=folder.name)
                    uid_tuple = [str(1), max_uid]


                    if max_id_of_exists_table is not None:
                        uid_tuple = [max_id_of_exists_table, max_uid]
                        if float(max_id_of_exists_table) > int(max_uid) * 0.8:
                            skip_current_folder = True
                    


                    if not skip_current_folder:
                        global_count =  int(max_id_of_exists_table) if max_id_of_exists_table is not None else 0
                        create_table_if_not_exist(folder=folder.name)
                        


                        mailbox.folder.set(folder=folder.name)

                        for msg in mailbox.fetch(imap.U(uid_tuple[0], uid_tuple[1])):
                            try:
                                print('global_count:', global_count)
                                print('currentMessageSubject:', msg.subject)
                                print('currentFolder:', folder.name)
                                print('maxUid')

                                message_data = generate_message_data(msg)

                                insert_message_data_to_db(message_data=message_data, folder=folder.name)
                                    
                                global_count += 1
                            except Exception as e:
                                print(f"Ошибка обработки сообщения: {str(e)}")
                        global_count = 0
                program_ends = True
        except Exception as e:
            print('ERROR:', e)
            input()
            
start()