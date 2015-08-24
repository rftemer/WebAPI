using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebApi.Model
{
    public class SKU
    {
        public string tipo { get; set; }
        public string dataEnvio { get; set; }
        public PARAMETROS parametros { get; set; }
    }
    public class PARAMETROS
    {
        public int idProduto { get; set; }
        public int idSku { get; set; }
    }
}