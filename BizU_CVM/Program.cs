using System;
using System.Diagnostics;

namespace BizU_CVM
{
    class Program
    {
        static void Main(string[] args)
        {
            LeituraArquivos leitura = new LeituraArquivos();
            FTP ftp = new FTP();

            var sw = new Stopwatch();
            sw.Start();
            //ftp.BaixarArquivo();
            //.insereDadosBanco();
            //leitura.fechaConexao();
            leitura.abordagemTeste();
            sw.Stop();

            Console.WriteLine($"Tempo Total = {sw.ElapsedMilliseconds} ms");
            Console.WriteLine($"Memória Utilizada = {Process.GetCurrentProcess().WorkingSet64 / 1024 / 1024}");
            
        }
    }
}
