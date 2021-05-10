using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;
using static System.Net.Mime.MediaTypeNames;


namespace BizU_CVM
{
    public class FTP
    {
        public void BaixarArquivo()
        {
            //string connectionString = "Server=localhost;Database=CVM;Trusted_Connection=True";            
            string pathDestino = @"C:\Users\wladi\Documents\ArquivosCVM";
            string path = @"C:\Users\wladi\Documents\ArquivosCVM\";
            DateTime data = DateTime.Now;
            var ano = data.Year;            
            int contador = 1;
            //Informacoes inf = new Informacoes();
            LeituraArquivos leitura = new LeituraArquivos();
            int qtdeArquivos = 0;


            //Lógica Para Baixar o DFP
            using (var client = new WebClient())
            {
                
                Console.WriteLine("Baixando Arquivos DFP...");
                for (int i = 2010; i < ano; i++)
                {
                    client.DownloadFile($"http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{i}.zip", $@"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_{i}.zip");
                    qtdeArquivos++;
                };
            }

            Thread.Sleep(3000);

            if (Directory.Exists(pathDestino))
            {                
                Console.Clear();
                Console.WriteLine("Extraindo Arquivos DFP...");
                for (int i = 2010; i < ano; i++)
                {
                    Console.WriteLine($"Extraindo {contador} de {qtdeArquivos}");
                    string nomeArquivo = $@"{path}dfp_cia_aberta_{i}.zip";                    

                    using (ZipArchive archive = ZipFile.OpenRead(@$"C:\Users\wladi\Documents\ArquivosCVM\dfp_cia_aberta_{i}.zip"))
                    {
                        foreach(ZipArchiveEntry entry in archive.Entries)
                        {
                            if (entry.FullName.Equals($"dfp_cia_aberta_BPA_con_{i}.csv"))
                            {                                
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);                                

                            }else if (entry.FullName.Equals($"dfp_cia_aberta_BPP_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }else if (entry.FullName.Equals($"dfp_cia_aberta_DFC_MI_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }else if (entry.FullName.Equals($"dfp_cia_aberta_DRE_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }
                        }
                    }
                    contador++;
                }
            }

            //Lógica Para Baixar o ITR
            using (var client = new WebClient())
            {
                qtdeArquivos = 0;
                contador = 1;
                Console.Clear();
                Console.WriteLine("Baixando Arquivos ITR...");
                for (int i = 2011; i < ano; i++)
                {
                    client.DownloadFile($"http://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/itr_cia_aberta_{i}.zip", $@"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_{i}.zip");
                    qtdeArquivos++;
                };
            }

            Thread.Sleep(3000);

            if (Directory.Exists(pathDestino))
            {
                Console.Clear();
                Console.WriteLine("Extraindo Arquivos ITR...");
                for (int i = 2011; i < ano; i++)
                {
                    Console.WriteLine($"Extraindo {contador} de {qtdeArquivos}");
                    string nomeArquivo = $@"{path}itr_cia_aberta_{i}.zip";

                    using (ZipArchive archive = ZipFile.OpenRead(@$"C:\Users\wladi\Documents\ArquivosCVM\itr_cia_aberta_{i}.zip"))
                    {
                        foreach (ZipArchiveEntry entry in archive.Entries)
                        {
                            if (entry.FullName.Equals($"itr_cia_aberta_BPA_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);

                            }
                            else if (entry.FullName.Equals($"itr_cia_aberta_BPP_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }
                            else if (entry.FullName.Equals($"itr_cia_aberta_DFC_MI_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }
                            else if (entry.FullName.Equals($"itr_cia_aberta_DRE_con_{i}.csv"))
                            {
                                string destinationPath = Path.GetFullPath(Path.Combine(pathDestino, entry.FullName));

                                if (destinationPath.StartsWith(pathDestino, StringComparison.Ordinal))
                                    entry.ExtractToFile(destinationPath);
                            }
                        }
                    }
                    contador++;
                }
            }            

            //Ler os arquivos que foram extraídos e jogar no banco de dados
            //DFP
            //try
            //{
            //    //Laço FOR para DFP_CIA_ABERTA_BPA_CON_ANODESEJADO
            //    for (int i = 2010; i < ano; i++)
            //    {
            //        var dfp_bpa_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\Extraidos\dfp_cia_aberta_BPA_con_{i}.csv");

            //        var enumLines = File.ReadLines(dfp_bpa_con_ano, Encoding.GetEncoding(28591));

            //        foreach (var line in enumLines)
            //        {
            //            var palavra = line.Split(';');
            //            if (palavra[0] != "CNPJ_CIA")
            //            {
            //                int cdcvm = int.Parse(palavra[4]);
            //                double vlconta = double.Parse(palavra[12]) / 1000000000;
            //                Console.WriteLine(palavra[0]);
            //                Console.WriteLine(palavra[1]);
            //                Console.WriteLine(palavra[2]);
            //                Console.WriteLine(palavra[3]);
            //                Console.WriteLine(cdcvm.ToString());
            //                Console.WriteLine(palavra[5]);
            //                Console.WriteLine(palavra[6]);
            //                Console.WriteLine(palavra[7]);
            //                Console.WriteLine(palavra[8]);
            //                Console.WriteLine(palavra[9]);
            //                Console.WriteLine(palavra[10]);
            //                Console.WriteLine(palavra[11]);
            //                Console.WriteLine(vlconta.ToString());
            //                Console.WriteLine(palavra[13]);

            //                string sql = "INSERT INTO DFPBPACON(cnpj_cia,dt_refer,versao,denom_cia,cd_cvm,grupo_dfp,moeda,escala_moeda,ordem_exerc,dt_fim_exerc,cd_conta,ds_conta,vl_conta,st_conta_fixa) VALUES('" + palavra[0] + "','" + palavra[1] + "','" + palavra[2] + "','" + palavra[3] + "','" + palavra[4] + "','" + palavra[5] + "','" + palavra[6] + "','" + palavra[7] + "','" + palavra[8] + "','" + palavra[9] + "','" + palavra[10] + "','" + palavra[11] + "','" + palavra[12] + "','" + palavra[13] + "')";

            //                SqlConnection con = new SqlConnection(connectionString);
            //                SqlCommand cmd = new SqlCommand(sql, con);
            //                cmd.CommandType = CommandType.Text;

            //                con.Open();

            //                cmd.ExecuteNonQuery();                            

            //            }

            //        }
            //    }

            //    ////Laço FOR para DFP_CIA_ABERTA_BPP_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var dfp_bpp_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\dfp_cia_aberta_BPP_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(dfp_bpp_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}

            //    ////Laço FOR para DFP_CIA_ABERTA_DFC_MI_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var dfp_dfc_mi_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\dfp_cia_aberta_DFC_MI_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(dfp_dfc_mi_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}

            //    ////Laço FOR para DFP_CIA_ABERTA_DRE_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var dfp_dre_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\dfp_cia_aberta_DRE_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(dfp_dre_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}


            //    ////ITR
            //    ////Laço FOR para ITR_CIA_ABERTA_BPA_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var itr_bpa_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\itr_cia_aberta_BPA_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(itr_bpa_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}

            //    ////Laço FOR para ITR_CIA_ABERTA_BPP_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var itr_bpp_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\itr_cia_aberta_BPP_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(itr_bpp_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}

            //    ////Laço FOR para ITR_CIA_ABERTA_DFC_MI_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var itr_dfc_mi_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\itr_cia_aberta_DFC_MI_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(itr_dfc_mi_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}

            //    ////Laço FOR para ITR_CIA_ABERTA_DRE_CON_ANODESEJADO
            //    //for (int i = 2010; i < ano; i++)
            //    //{
            //    //    var itr_dre_con_ano = ($@"C:\Users\wladi\OneDrive\Biz_U Startup\CVM\ArquivosExtraidos\itr_cia_aberta_DRE_con_{i}.csv");


            //    //    var enumLines = File.ReadLines(itr_dre_con_ano, Encoding.GetEncoding(28591));

            //    //    foreach (var line in enumLines)
            //    //    {
            //    //        var palavra = line.Split(';');
            //    //        if (palavra[0] != "CNPJ_CIA")
            //    //        {
            //    //            Console.WriteLine(palavra[0]);
            //    //            Console.WriteLine(palavra[1]);
            //    //            Console.WriteLine(palavra[2]);
            //    //            Console.WriteLine(palavra[3]);
            //    //            Console.WriteLine(palavra[4]);
            //    //            Console.WriteLine(palavra[5]);
            //    //            Console.WriteLine(palavra[6]);
            //    //            Console.WriteLine(palavra[7]);
            //    //            Console.WriteLine(palavra[8]);
            //    //            Console.WriteLine(palavra[9]);
            //    //            Console.WriteLine(palavra[10]);
            //    //            Console.WriteLine(palavra[11]);
            //    //            Console.WriteLine(palavra[12]);
            //    //            Console.WriteLine(palavra[13]);

            //    //        }

            //    //    }
            //    //}
            //}catch(Exception ex)
            //{
            //    Console.WriteLine(ex.Message);
            //}
        }
    }
}
