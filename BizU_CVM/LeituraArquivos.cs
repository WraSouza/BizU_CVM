using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace BizU_CVM
{
    public class LeituraArquivos
    {
        public void insereDadosBanco()
        {
            string connectionString = "Server=localhost;Database=CVM;Trusted_Connection=True";
            SqlConnection con = new SqlConnection(connectionString);
            try
            {                
                con.Open();
                DateTime data = DateTime.Now;
                var ano = data.Year;
                string line;
                string cnpj_cia;
                string dt_refer;
                string versao;
                string denom_cia;
                string cd_cvm;
                string grupo_dfp;
                string moeda;
                string escala_moeda;
                string ordem_exerc;
                string dt_ini_exerc;
                string dt_fim_exerc;
                string cd_conta;
                string ds_conta;
                string vl_conta;
                string st_conta_fixa;                

                //DFP BPA CON
                for (int i = 2010; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_BPA_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Onze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Doze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Treze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Quatorze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);
                                string sql = "INSERT INTO DFPBPACON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //Ler BPP CON
                for (int i = 2010; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_BPP_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Onze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Doze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Treze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Quatorze Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);
                                
                                string sql = "INSERT INTO DFPBPPCON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //Ler DFC MI
                for (int i = 2010; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_DFC_MI_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_ini_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Primeira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();
                               

                                //Décima-Quinta Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();                               

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " "+dt_ini_exerc+" " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);


                                string sql = "INSERT INTO DFPDFCMICON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_ini_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_INI_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_INI_EXERC", dt_ini_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //Ler DRE CON
                for (int i = 2010; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_DRE_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_ini_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Primeira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Décima-Quinta Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_ini_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);


                                string sql = "INSERT INTO DFPDRECON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_ini_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_INI_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_INI_EXERC", dt_ini_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //Ler ITR BPA
                for (int i = 2011; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_BPA_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Onze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Doze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Treze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Quatorze Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);

                                string sql = "INSERT INTO ITRBPACON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //ITR BPP CON
                for (int i = 2011; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_BPP_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Onze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Doze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Treze Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Quatorze Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);

                                string sql = "INSERT INTO ITRBPPCON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //ITR DFC MI
                for (int i = 2011; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_DFC_MI_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_ini_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Primeira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                               

                                //Décima-Quinta Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_ini_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);


                                string sql = "INSERT INTO ITRDFCMICON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_ini_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_INI_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_INI_EXERC", dt_ini_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }

                //ITR DRE CON
                for (int i = 2011; i < ano; i++)
                {
                    using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_DRE_con_{i}.csv"))
                    using (var reader = new StreamReader(fs, Encoding.GetEncoding(28591)))
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (!(line.StartsWith("CNPJ_CIA")))
                            {
                                int posicaoSeparador = 0;
                                var span = line.AsSpan();

                                //Primeira Coluna
                                posicaoSeparador = span.IndexOf(';');
                                cnpj_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_refer = span.Slice(0, posicaoSeparador).ToString();

                                //Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                versao = span.Slice(0, posicaoSeparador).ToString();

                                //Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                denom_cia = span.Slice(0, posicaoSeparador).ToString();

                                //Quinta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_cvm = span.Slice(0, posicaoSeparador).ToString();

                                //Sexta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                grupo_dfp = span.Slice(0, posicaoSeparador).ToString();

                                //Sétima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Oitava Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                escala_moeda = span.Slice(0, posicaoSeparador).ToString();

                                //Nona Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ordem_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_ini_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Primeira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                dt_fim_exerc = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Segunda Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                cd_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Terceira Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                ds_conta = span.Slice(0, posicaoSeparador).ToString();

                                //Décima-Quarta Coluna
                                span = span.Slice(posicaoSeparador + 1);
                                posicaoSeparador = span.IndexOf(';');
                                vl_conta = span.Slice(0, posicaoSeparador).ToString();                                

                                //Décima-Quinta Coluna
                                span = span.Slice(posicaoSeparador);
                                posicaoSeparador = span.IndexOf(';');
                                st_conta_fixa = span.ToString();

                                Console.WriteLine(cnpj_cia + " " + dt_refer + " " + versao + " " + denom_cia + " " + cd_cvm + " " + grupo_dfp + " " + moeda + " " + escala_moeda + " " + ordem_exerc + " " + dt_ini_exerc + " " + dt_fim_exerc + " " + cd_conta + " " + ds_conta + " " + vl_conta + " " + st_conta_fixa);


                                string sql = "INSERT INTO ITRDRECON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_ini_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) " +
                                    "VALUES(@CNPJ_CIA,@DT_REFER,@VERSAO,@DENOM_CIA,@CD_CVM,@GRUPO_DFP,@MOEDA,@ESCALA_MOEDA,@ORDEM_EXERC,@DT_INI_EXERC,@DT_FIM_EXERC,@CD_CONTA,@DS_CONTA,@VL_CONTA,@ST_CONTA_FIXA)";


                                SqlCommand cmd = new SqlCommand(sql, con);
                                cmd.Parameters.AddWithValue("@CNPJ_CIA", cnpj_cia);
                                cmd.Parameters.AddWithValue("@DT_REFER", dt_refer);
                                cmd.Parameters.AddWithValue("@VERSAO", versao);
                                cmd.Parameters.AddWithValue("@DENOM_CIA", denom_cia);
                                cmd.Parameters.AddWithValue("@CD_CVM", cd_cvm);
                                cmd.Parameters.AddWithValue("@GRUPO_DFP", grupo_dfp);
                                cmd.Parameters.AddWithValue("@MOEDA", moeda);
                                cmd.Parameters.AddWithValue("@ESCALA_MOEDA", escala_moeda);
                                cmd.Parameters.AddWithValue("@ORDEM_EXERC", ordem_exerc);
                                cmd.Parameters.AddWithValue("@DT_INI_EXERC", dt_ini_exerc);
                                cmd.Parameters.AddWithValue("@DT_FIM_EXERC", dt_fim_exerc);
                                cmd.Parameters.AddWithValue("@CD_CONTA", cd_conta);
                                cmd.Parameters.AddWithValue("@DS_CONTA", ds_conta);
                                cmd.Parameters.AddWithValue("@VL_CONTA", vl_conta);
                                cmd.Parameters.AddWithValue("@ST_CONTA_FIXA", st_conta_fixa);
                                //cmd.CommandType = CommandType.Text;

                                cmd.ExecuteNonQuery();

                            }

                        }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                con.Close();
            }

        }

        public void fechaConexao()
        {
            string connectionString = "Server=localhost;Database=CVM;Trusted_Connection=True";
            SqlConnection con = new SqlConnection(connectionString);
            con.Close();
        }

        public void abordagemTeste()
        {
            DateTime data = DateTime.Now;
            int ano = data.Year;
            var rawBuffer = new byte[1024 * 1024];
            //var cnpj_cia;
            string dt_refer;
            string versao;
            string denom_cia;
            string cd_cvm;
            string grupo_dfp;
            string moeda;
            string escala_moeda;
            string ordem_exerc;
            string dt_ini_exerc;
            string dt_fim_exerc;
            string cd_conta;
            string ds_conta;
            string vl_conta;
            string st_conta_fixa;
            //DFP
            for (int i=2010; i < ano; i++)
            {
                using (var fs = File.OpenRead($@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_BPA_con_{i}.csv"))
                {
                    var bytesBuffered = 0;
                    var bytesConsumed = 0;

                    while (true)
                    {
                        var bytesRead = fs.Read(rawBuffer, bytesBuffered, rawBuffer.Length - bytesBuffered);

                        if (bytesRead == 0) break;
                        bytesBuffered += bytesRead;

                        int linePosition;

                        do
                        {
                            linePosition = Array.IndexOf(rawBuffer, (byte)'\n', bytesConsumed,
                bytesBuffered - bytesConsumed);

                            if (linePosition > 0)
                            {
                                var lineLength = linePosition - bytesConsumed;
                                var line = new Span<byte>(rawBuffer, bytesConsumed, lineLength);
                                bytesConsumed += lineLength + 1 ;

                                var span = line.Slice(line.IndexOf((byte)';')+1);

                                var firstCommaPos = span.IndexOf((byte)';');

                                var cnpj_cia = span.Slice(0, firstCommaPos);
                                Console.WriteLine(Encoding.UTF8.GetString(cnpj_cia));
                                var CNPJ = (Encoding.UTF8.GetString(cnpj_cia));
                            }
                        } while (linePosition >= 0);
                    }

                }
            }
            
        }
    }
}
