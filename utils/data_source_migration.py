# -*- coding: utf-8 -*-
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

import argparse
import json
import sys

def connect_to_database(db_url):
    """
    Conecta ao banco de dados usando a URL fornecida e retorna uma sessão e metadados.
    
    Args:
        db_url (str): URL de conexão do banco de dados.
    
    Returns:
        session: Sessão para executar operações no banco de dados.
        metadata: Objeto MetaData contendo informações sobre as tabelas.
    """
    try:
        if "/limonero" not in db_url:
            db_url += "/limonero"
        
        try:
            engine = create_engine(db_url, echo=False)
        except:
            import pymysql
            pymysql.install_as_MySQLdb()
            engine = create_engine(db_url, echo=False)
            
        Session = sessionmaker(bind=engine)
        session = Session()

        metadata = MetaData()
        metadata.reflect(bind=engine)  # Carrega as informações do esquema do banco de dados
        
        print(f"Conexão bem-sucedida ao banco de dados `{db_url}`!")
        return session, metadata
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

def execute_custom_query(session, query, single_row=False):
    """
    Executa uma query SQL personalizada e retorna os resultados.

    Args:
        session (Session): Sessão ativa do SQLAlchemy.
        query (str): Query SQL a ser executada.
        single (boolean): Se o retorno será uma única linha ou várias.

    Returns:
        list: Lista de resultados como dicionários.
    """
    try:
        results = session.execute(text(query))
        if single_row:
            data = results.fetchone()
        else:
            data = results.fetchall()

        if not data:
            data = []
        return data
    except Exception as e:
        print(f"Erro ao executar a query: {e}")
        return []
    

def get_current_id(session, full_table_name):
    """
    Recupera qual é o maior `id` de uma tabela.
    
    Args:
        session (Session): Sessão ativa do SQLAlchemy.
        full_table_name (str): Nome da tabela alvo no formato `database.table`.
    
    Returns:
        current_id (int): O id mais atual da tabela.
    """
    query = f"SELECT MAX(id) FROM {full_table_name};"
    current_id = execute_custom_query(session, query, single_row=True)[0]
    if not current_id:
        current_id = 0
    return current_id 


def get_user_info(session, user_login):
    """
    Recupera informações do usuário no banco de dados alvo baseado no seu login.
    
    Args:
        session (Session): Sessão ativa do SQLAlchemy.
        user_login (str): Login salvo no Thorn.
        
    Returns:
        dict: Um dicionário contendo informações do user_id, user_name e user_login
    
    """
    query = f"SELECT id, first_name FROM thorn.user WHERE login = '{user_login}';"
    r = execute_custom_query(session, query, single_row=True)
    if r:
        return {'user_id': r[0], "user_name": r[1], "user_login": user_login}
    else:
        raise Exception(f"User with login `{user_login}` not found in target!")

        
def get_all_ids(session, full_table_name):
    """
    Recupera a lista de ids de uma tabela.
    
    Args:
        session (Session): Sessão ativa do SQLAlchemy.
        full_table_name (str): Nome da tabela alvo no formato `database.table`.
    
    Returns:
        list: Lista de ids da tabela.
    """
    query = f"SELECT id FROM {full_table_name};"
    ids_list = execute_custom_query(session, query)
    return [i[0] for i in ids_list]


def migrate_limonero_data_source(source_session, source_data_source_id, target_session, target_metadata, mapping_info, user):
    """
    Recupera as informações da tabela limonero.data_source, atualizando os ids no alvo.
        
    Returns:
        list: dados a serem inseridos na tabela alvo.
    """
    

    table = target_metadata.tables['data_source']
    columns = [c.name for c in table.columns]

    query = f"SELECT * FROM limonero.data_source WHERE id = {source_data_source_id};"

    row = dict(zip(columns, execute_custom_query(source_session, query, single_row=True))) 
                 
    row['id'] = mapping_info['data_source'][source_data_source_id]
    row['storage_id'] = mapping_info['storage'][int(row['storage_id'])]

    if user:
        row['user_id'] = user['user_id']
        row['user_login'] = user['user_login']
        row['user_name'] = user['user_name']

    return [table.insert(), [row]], mapping_info

def migrate_limonero_data_source_var(source_session, source_data_source_id, target_session, target_metadata, mapping_info):
    """
    Recupera as informações da tabela limonero.data_source_variable, atualizando os ids no alvo.
        
    Returns:
        list: dados a serem inseridos na tabela alvo.
    """
    

    table = target_metadata.tables['data_source_variable']
    columns = [c.name for c in table.columns]

    query = f"SELECT * FROM limonero.data_source_variable WHERE data_source_id = {source_data_source_id};"

    move_data = [dict(zip(columns, row)) 
                 for row in execute_custom_query(source_session, query)]

    for row in move_data:
        row['id'] = None
        row['data_source_id'] = mapping_info['data_source'][source_data_source_id]

    return table.insert(), move_data


def migrate_limonero_attribute(source_session, source_data_source_id, target_session, target_metadata, mapping_info):
    """
    Recupera as informações da tabela limonero.attribute, atualizando os ids no alvo.
        
    Returns:
        list: dados a serem inseridos na tabela alvo.
    """
    

    table = target_metadata.tables['attribute']
    columns = [c.name for c in table.columns]

    query = f"SELECT * FROM limonero.attribute WHERE data_source_id = {source_data_source_id};"

    move_data = [dict(zip(columns, row)) 
                 for row in execute_custom_query(source_session, query)]

    for row in move_data:
        row['id'] = None
        row['data_source_id'] = mapping_info['data_source'][source_data_source_id]

    return table.insert(), move_data


def main():
    parser = argparse.ArgumentParser(
        description="Script to migrate Lemonade's data sources from one database to another."
    )

    parser.add_argument(
        "--source-db",
        required=True,
        help="Connection string for the source database (e.g., 'mysql://user:pass@host:port')."
    )

    parser.add_argument(
        "--target-db",
        required=True,
        help="Connection string for the target database (e.g., 'mysql://user:pass@host:port')."
    )
    
    parser.add_argument(
        "--storage",
        type=str,
        required=True,
        help="The storage id to link all datasources or a mapping like `4:1,1:2` meaning: all datasources from origin storage_id 4 will be mapped to storage_id 1 in target."
    )
    
    parser.add_argument(
        "--id",
        type=str,
        required=True,
        help="ID of the datasources to migrate. Supported options are: a specific id (e.g., `1`), a list of ids (e.g., `1,2,3`), or `all` to migrate all."
    )
    
    parser.add_argument(
        "--user-login",
        type=str,
        required=False,
        help="When set, it will change the current user owner of the data source."
    )
    
    args = parser.parse_args()

    print("Migration Configuration:")
    print(f"  Source Database: {args.source_db}")
    print(f"  Target Database: {args.target_db}")
    print(f"  Data Source: {args.id}")
    print(f"  Storage mapping: {args.storage}")
    print(f"  User login: {args.user_login}")
    
    source_session, source_metadata = connect_to_database(args.source_db)
    target_session, target_metadata = connect_to_database(args.target_db)    
    
    if args.user_login:
        user_info = get_user_info(target_session, args.user_login)
    else:
        user_info = None
        
    if args.id == "all":
        ids_list = get_all_ids(source_session, f'limonero.data_source')
    elif "," in args.id:
        ids_list = [int(p.strip()) for p in args.id.split(",")]
    else:
        ids_list = [int(args.id)]

    print(f"Data Source's ids to copy: {ids_list}")
    
    values_to_copy = {'data_source': [], 'attribute': [], 'data_source_variable': []}
    mapping_info = {'data_source': {}, 'storage': {}}

    storages = args.storage.split(",")
    for c in storages:
        if ":" in c:
            s,t = c.split(":")
            mapping_info['storage'][int(s)] = int(t)
        else:
            print("ERROR! in storage parameter !", args.storage)  


    target_data_source_id = get_current_id(target_session, "limonero.data_source")
    for source_data_source_id in ids_list:
        target_data_source_id += 1   
        print(f"Cloning data_source from source {source_data_source_id} to {target_data_source_id} on target.")
        mapping_info['data_source'][source_data_source_id] = target_data_source_id

    current_attribute_id = get_current_id(target_session, "limonero.attribute") # last id on target
    current_var_id = get_current_id(target_session, "limonero.data_source_variable") 
    for source_data_source_id in ids_list:

        result, mapping_info = migrate_limonero_data_source(source_session=source_session, source_data_source_id=source_data_source_id, target_session=target_session, 
                                                            target_metadata=target_metadata, mapping_info=mapping_info, user=user_info)
        values_to_copy['data_source'].append(result)

        print("Cloning data_source_variable ...")
        result = migrate_limonero_data_source_var(source_session=source_session, source_data_source_id=source_data_source_id, target_session=target_session, 
                                                  target_metadata=target_metadata, mapping_info=mapping_info)
        values_to_copy['data_source_variable'].append(result)
        
        print("Cloning attribute ...")              
        result = migrate_limonero_attribute(source_session=source_session, source_data_source_id=source_data_source_id, target_session=target_session, 
                           target_metadata=target_metadata, mapping_info=mapping_info)
        values_to_copy['attribute'].append(result)


    
    print("Starting migration process...")

    try:
        for item in ['data_source', 'attribute', 'data_source_variable']:
            migrations = values_to_copy.get(item)
            for migration in migrations:
                if migration:
                    table_ob, data = migration
                    if len(data) > 0:
                       print("############")                   
                       print(table_ob)
                       print(data)
                       print("############")
                       target_session.execute(table_ob, data)
                       target_session.flush()
    except:
        target_session.rollback()
        source_session.close()
        target_session.close()
        raise

    target_session.commit()
    source_session.close()
    target_session.close()
    print("Migration completed successfully!")

if __name__ == "__main__":
    main()
