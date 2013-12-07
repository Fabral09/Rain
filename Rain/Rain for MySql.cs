﻿using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Data;
using MySql.Data;
using MySql.Data.MySqlClient;
using MySql.Data.Types;

namespace Rain
{
    namespace MySQL
    {
        /// <summary>
        /// Rain Engine per MySQL
        /// </summary>
        public class RainEngine
        {
            private MySqlConnectionStringBuilder string_builder = null;
            private MySqlConnection conn = null;
            private MySqlCommand cmd = null;
            private MySqlDataAdapter da = null;
            private bool aWorkingTableIsSet = false;
            private string workingTable = null;
            
            #region Costruttori / Distruttore

            /// <param name="hostname"> Nome dell'istanza MySQL a cui bisogna collegarsi </param>
            /// <param name="dbname"> Nome del database </param>
            /// <param name="username"> Nome utente </param>
            /// <param name="password"> Password dell'utente </param>

            public RainEngine(string hostname, string dbname, string username = "root", string password = "")
            {
                string_builder = new MySqlConnectionStringBuilder();
                // Imposto i parametri di connessione per la connection string
                string_builder.Server = hostname;
                string_builder.Database = dbname;
                string_builder.UserID = username;
                if (!password.Trim().Equals("")) string_builder.Password = password;
            }

            ~RainEngine()
            {
                if (conn != null && conn.State.Equals(ConnectionState.Open))
                    conn.Close();
            }

            #endregion

            #region Apertura / Chiusura connessione

            /// <summary>
            /// Apre la connessione 
            /// </summary>
            /// <returns>Restituisce true se l'apertura della connessione và a buon fine, false altrimenti</returns>

            public bool OpenConnection()
            {
                conn = new MySqlConnection();
                // Recupero la connection string
                conn.ConnectionString = this.string_builder.ConnectionString;
                
                try
                {
                    // Apro la connessione
                    conn.Open();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }

            /// <summary>
            /// Chiude la connessione 
            /// </summary>
            /// <returns>Restituisce true se la chiusura della connessione và a buon fine, false altrimenti</returns>
           
            public bool CloseConnection()
            {
                try
                {
                    // Chiudo la connessione
                    this.conn.Close();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }

            #endregion

            #region Metodi per la selezione dati

            /// <summary>
            /// Effettua una SELECT su una tabella e restituisce i valori sottoforma di oggetto DataSet
            /// </summary>
            /// <param name="condition"> Condizione da impostare sulla query </param>
            /// <param name="row_limit"> Numero di righe restituite dalla query </param>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce il recordset ricavato dalla query sotto forma di DataSet in caso di successo. Altrimenti null </returns>
            
            public DataSet SelectAll(string condition = "", int row_limit = 0, string table = null)
            {
                DataSet ds = new DataSet();
                cmd = new MySqlCommand();
                da = new MySqlDataAdapter();
                string query = "SELECT";

                // Se presente il parametro per il limite di righe aggiungo alla query la clausola TOP
                if (row_limit != 0)
                    query += String.Format(" TOP {0} * ", row_limit.ToString()); 
                else
                    query += " * ";

                query += String.Format("FROM {0}", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Se presente la condizione aggiungo alla query la clausola WHERE
                if (!condition.Trim().Equals(""))
                    query += String.Format(" WHERE {0}", condition.Trim());

                cmd.Connection = this.conn;
                cmd.CommandText = query;
                da.SelectCommand = cmd;

                try
                {
                    // Riempio il DataSet con il recordset restituito dalla query
                    da.Fill(ds);
                }
                catch (MySqlException ex)
                {
                    return null;
                }

                return ds;
            }

            /// <summary>
            /// Effettua una SELECT su una tabella e restituisce i valori sottoforma di oggetto DataSet
            /// </summary>
            /// <param name="fields"> Elenco dei campi da far restiruire dalla query </param>
            /// <param name="condition"> Condizione da impostare sulla query </param>
            /// <param name="row_limit"> Numero di righe restituite dalla query </param>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce il recordset ricavato dalla query sotto forma di DataSet in caso di successo. Altrimenti null </returns>

            public DataSet Select(string[] fields, string condition = "", int row_limit = 0, string table = null)
            {
                DataSet ds = new DataSet();
                cmd = new MySqlCommand();
                da = new MySqlDataAdapter();
                string query = "SELECT ";

                // Se presente il parametro per il limite di righe aggiungo alla query la clausola TOP
                if (row_limit != 0)
                    query += String.Format("TOP {0} ", row_limit.ToString());

                // Aggiungo alla query l'elenco dei campi che voglio farmi restituire
                for (int i = 0; i < fields.Length; i++)
                    query += String.Format("{0}, ", fields[i].ToString());

                query = query.Remove(query.Length - 2, 2);
                query += String.Format(" FROM {0}", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Se presente la condizione aggiungo alla query la clausola WHERE
                if (!condition.Trim().Equals(""))
                    query += String.Format(" WHERE {0}", condition.Trim());

                cmd.Connection = this.conn;
                cmd.CommandText = query;
                da.SelectCommand = cmd;

                try
                {
                    // Riempio il DataSet con il recordset restituito dalla query
                    da.Fill(ds);
                }
                catch (MySqlException ex)
                {
                    return null;
                }

                return ds;
            }

            #endregion

            #region Metodi per la cancellazione dati

            /// <summary>
            /// Effettua un DELETE su una tabella
            /// </summary>
            /// <param name="condition"> Condizione da impostare sulla query </param>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce il valore true in caso di successo. Altrimenti false </returns>
            
            public bool Delete(string condition = "", string table = null)
            {
                cmd = new MySqlCommand();
                string query = "DELETE ";

                query += String.Format("FROM {0}", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Se presente la condizione aggiungo alla query la clausola WHERE
                if (!condition.Trim().Equals(""))
                    query += String.Format(" WHERE {0}", condition.Trim());

                cmd.Connection = this.conn;
                cmd.CommandText = query;

                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }

            /// <summary>
            /// Effettua un DROP su una tabella
            /// </summary>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce il valore true in caso di successo. Altrimenti false </returns>

            public bool Drop(string table = null)
            {
                cmd = new MySqlCommand();
                string query = String.Format("DROP TABLE {0} ", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));
                cmd.Connection = this.conn;
                cmd.CommandText = query;

                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }
            
            #endregion

            #region Metodi per l'aggiornamento dei dati
            
            /// <summary>
            /// Effettua un UPDATE su una tabella
            /// </summary>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <param name="fields"> Elenco dei campi da aggiornare </param>
            /// <param name="values"> Valori da utilizzare per l'aggiornamento </param>
            /// <param name="condition"> Condizione da impostare sulla query </param>
            /// <returns>Restituisce true in caso di successo. Altrimenti false </returns>

            public bool Update(string[] fields, object[] values, string condition = "", string table = null)
            {
                // Se il numero di parametri non coincide col numero di valori esco dal metodo
                if (fields.Length != values.Length) return false;

                cmd = new MySqlCommand();
                string query = String.Format("UPDATE {0} SET ", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Aggiungo alla query l'elenco degli aggiornamenti da effettuare nel formato CAMPO = NUOVO VALORE
                for (int i = 0; i < fields.Length; i++)
                {
                    string valore_corrente = null;

                    if (values[i] == null)
                        valore_corrente += "NULL";
                    else if (values[i].GetType().ToString().Equals("System.String"))
                        valore_corrente += "'" + values[i].ToString() + "'";
                    else
                        valore_corrente += String.Format("{0}", values[i].ToString());

                    query += String.Format("{0} = {1}, ", fields[i], valore_corrente);
                }
                query = query.Remove(query.Length - 2, 2);

                // Se presente la condizione aggiungo alla query la clausola WHERE
                if (!condition.Trim().Equals(""))
                    query += String.Format(" WHERE {0}", condition.Trim());

                cmd.Connection = this.conn;
                cmd.CommandText = query;
                
                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }

            /// <summary>
            /// Effettua un UPDATE su una tabella
            /// </summary>
            /// <param name="field_values">Struttura chiave - valore che contiene i dati per l'aggiornamento</param>
            /// <param name="condition"> Condizione da impostare sulla query </param>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce true in caso di successo. Altrimenti false </returns>

            public bool Update(Hashtable field_values, string condition = "", string table = null)
            {

                cmd = new MySqlCommand();
                string query = String.Format("UPDATE {0} SET ", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Aggiungo alla query l'elenco degli aggiornamenti da effettuare nel formato CAMPO = NUOVO VALORE
                foreach (DictionaryEntry kv in field_values)
                {
                    string valore_corrente = null;

                    if (kv.Value == null)
                        valore_corrente += "NULL";
                    else if (kv.Value.GetType().ToString().Equals("System.String"))
                        valore_corrente += "'" + kv.Value.ToString() + "'";
                    else
                        valore_corrente += String.Format("{0}", kv.Value.ToString());

                    query += String.Format("{0} = {1}, ", kv.Key.ToString(), valore_corrente);
                }
                query = query.Remove(query.Length - 2, 2);

                // Se presente la condizione aggiungo alla query la clausola WHERE
                if (condition != null && !condition.Trim().Equals(""))
                    query += String.Format(" WHERE {0}", condition.Trim());

                cmd.Connection = this.conn;
                cmd.CommandText = query;

                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }



            #endregion

            #region Metodi per l'inserimento dati

            /// <summary>
            /// Effettua una INSERT su una tabella
            /// </summary>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <param name="fields"> Elenco dei campi in cui inserire i valori </param>
            /// <param name="values"> Valori da aggiungere alla tabella </param>
            /// <returns>Restituisce true in caso di successo. Altrimenti false </returns>

            public bool Insert(string[] fields, object[] values, string table = null)
            {
                if (fields.Length != values.Length) return false;

                cmd = new MySqlCommand();
                string query = String.Format("INSERT INTO {0} (", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                // Aggiungo alla query l'elenco dei campi in cui inserire i nuovi valori
                for (int i = 0; i < fields.Length; i++)
                    query += String.Format("{0}, ", fields[i].ToString());

                query = query.Remove(query.Length - 2, 2);
                query += ") VALUES (";

                // Aggiungo alla query l'elenco dei nuovi valori da aggiungere
                object valore = null;
                for (int i = 0; i < values.Length; i++)
                {
                    if (values[i] == null)
                        valore = "NULL";
                    else if (values[i].GetType().ToString().Equals("System.String"))
                        valore = "'" + values[i].ToString() + "'";
                    else
                        valore = values[i].ToString();

                    query += String.Format("{0}, ", valore);
                }
                query = query.Remove(query.Length - 2, 2);
                query += ")";
                cmd.Connection = this.conn;
                cmd.CommandText = query;
                
                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }
            
            /// <summary>
            /// Effettua una INSERT su una tabella
            /// </summary>
            /// <param name="fields_values">Struttura chiave - valore che contiene i dati per l'aggiornamento</param>
            /// <param name="table"> Nome della tabella su cui effettuare la query </param>
            /// <returns>Restituisce true in caso di successo. Altrimenti false </returns>

            public bool Insert(Hashtable fields_values, string table = null)
            {
                cmd = new MySqlCommand();
                string query = String.Format("INSERT INTO {0} (", ((this.aWorkingTableIsSet == true) ? this.workingTable.Trim() : table.Trim()));

                string campi = "", valori = "";

                // Creo due stringhe: una che contiene l'elenco dei campi da utilizzare nella INSERT, e un'altra che contiene i valori
                foreach (DictionaryEntry kv in fields_values)
                {
                    campi += String.Format("{0}, ", kv.Key.ToString());
                    if (kv.Value == null)
                        valori += "NULL,";
                    else if (kv.Value.GetType().ToString().Equals("System.String"))
                        valori += "'" + kv.Value.ToString() + "', ";
                    else
                        valori += String.Format("{0}, ", kv.Value.ToString());
                }

                valori = valori.Remove(valori.Length - 2, 2);
                campi = campi.Remove(campi.Length - 2, 2);
                query += campi + ") VALUES (" + valori;

                query += ")";
                cmd.Connection = this.conn;
                cmd.CommandText = query;

                try
                {
                    // Eseguo la query
                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException ex)
                {
                    return false;
                }

                return true;
            }

            #endregion

            #region Metodi aggiuntivi

            /// <summary>
            /// Imposta una Working Table
            /// </summary>
            /// <param name="table"> Nome della tabella da impostare come Working Table </param>

            public void SetWorkingTable(string table)
            {
                this.aWorkingTableIsSet = true;
                this.workingTable = table;
            }

            /// <summary>
            /// Elimina la Working Table corrente
            /// </summary>
            
            public void UnSetWorkingTable()
            {
                this.aWorkingTableIsSet = false;
                this.workingTable = null;
            }

            #endregion

        }
    }
}
