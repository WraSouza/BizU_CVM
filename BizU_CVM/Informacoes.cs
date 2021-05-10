using System;
using System.Collections.Generic;
using System.Text;

namespace BizU_CVM
{
    public class Informacoes
    {
        public string nomeArquivo { get; set; }
        public string cnpj_cia { get; set; }
        public string dt_refer { get; set; }
        public string versao { get; set; }
        public string denom_cia { get; set; }
        public int cd_cvm { get; set; }
        public string grupo_dfp { get; set; }
        public string moeda { get; set; }
        public string escala_moeda { get; set; }
        public string ordem_exerc { get; set; }
        public string dt_fim_exerc { get; set; }
        public string cd_conta { get; set; }
        public string ds_conta { get; set; }
        public double vl_conta { get; set; }
        public string st_conta_fixa { get; set; }

        public override string ToString()
        {
            //return base.ToString();
            return cnpj_cia.ToString() + vl_conta.ToString() + st_conta_fixa.ToString();
        }
    }
}
