#*******************************************************************************************************#
#-----------------------------------------Tavaux Pratiques SID -----------------------------------------#
#---------------------------------------FORMATION: DATA STEWARDS----------------------------------------#                      
# @@@@@@@@@@@@@@@@@@@@@@_____________ Auteur: Hubert Azonvidé DOSSA_____________________________________#
#
# 1 Importation des Bibliothèques Python 
import pandas as pd
import mysql.connector
from mysql.connector import Error


## Importation de la prémière table *fraud_test* de notre base de donnée 

test_fraud= pd.read_csv("C:/Formation_Data_Steward_Minisitère/Système_Info_Décisionnel/TP_Individuel/fraudTest.csv")

print('******************** 1 ère Table importée avec succès**************************')


# Afficher les columnes à prendre en compte
 
print(test_fraud.columns)

## Importation de la deuxième table *fraud_train* de notre base de donnée 

train_fraud= pd.read_csv("C:/Formation_Data_Steward_Minisitère/Système_Info_Décisionnel/TP_Individuel/fraudTrain.csv")

print('*********************** 2è Table importée avec succès************************')


# Afficher les columnes à prendre en compte
 
print(train_fraud.columns)

# Chargement du dataframe dans MYSQL
try:
    connexion = mysql.connector.connect(host='localhost',
                                       database='donnee_fraud',
                                       user='root',
                                       password='')
    
    if connexion.is_connected():
        print('Connexion à MySQL réussie')

except Error as e:

    print(f"Erreur lors de la connexion à MySQL: {e}")



# Définir les DataFrames test_fraud et train_fraud ici

try:
    # Connexion à MySQL
    """
    Si la connexion est réussie, les données du premier et du deuxième DataFrame 
    sont insérées dans les  tables MySQL table_test et table_train respectivement.
    """
    connexion = mysql.connector.connect(host='localhost',
                                       database='donnee_fraud',
                                       user='root',
                                       password='')
    if connexion.is_connected():
        print('Connexion à MySQL réussie')

    # Chargement du premier DataFrame dans MySQL
    cursor = connexion.cursor()
    for i, row in test_fraud.iterrows():
        sql = """INSERT INTO table_test (id, trans_date_trans_time, cc_num, merchant, category, amt, first, 
        last, gender, street, city, state, zip, lat, longu, city_pop, job, dob, trans_num, unix_time, merch_lat,
        merch_long, is_fraud) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.execute(sql, tuple(row))
    connexion.commit()
    print("Premier DataFrame chargé dans MySQL avec succès!")

    # Chargement du deuxième DataFrame dans MySQL
    for i, row in train_fraud.iterrows():
        sql = """INSERT INTO table_train (id, trans_date_trans_time, cc_num, merchant, category, amt, first, 
        last, gender, street, city, state, zip, lat, longu, city_pop, job, dob, trans_num, unix_time, merch_lat,
        merch_long, is_fraud) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        cursor.execute(sql, tuple(row))
    connexion.commit()
    print("Deuxième DataFrame chargé dans MySQL avec succès!")


#============ En cas d'erreur lors de la connexion ou de l'insertion des données, l'exception est capturée et affiché
except Error as e:
    print(f"Erreur lors de la connexion à MySQL: {e}")


#============= s'assurer que la connexion à la base de données est fermée proprement, même en cas d'erreur ou de succès.   
finally:
    if connexion.is_connected():
        cursor.close()
        connexion.close()
        print("Connexion à MySQL fermée")
