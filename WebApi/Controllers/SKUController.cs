using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using WebApi.Model;
using MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;
using MongoDB.Driver.Linq;
using MongoDB.Driver;
using MongoDB.Bson;
using MongoDB.Driver.Wrappers;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;



namespace WebApi.Controllers
{
    public class SKUController : ApiController
    {
        List<SKU> data;
        public SKUController()
        {
            try
            {
                data = new List<SKU>();
                WebRequest request = WebRequest.Create("https://s3-us-west-2.amazonaws.com/desafiotecnico/criacao_sku.json");
                WebResponse response = request.GetResponse();
                Stream dataStream = response.GetResponseStream();

                StreamReader reader = new StreamReader(dataStream);

                string responseFromServer = reader.ReadToEnd();

                JArray json = JArray.Parse(responseFromServer);
                foreach (JObject item in json.Children<JObject>())
                {
                    SKU sku = JsonConvert.DeserializeObject<SKU>(item.ToString());
                    data.Add(sku);

                }

                reader.Close();
                response.Close();
            }
            catch(Exception ex)
            {
               
            }
         }

        //POST api/sku/?{...
        public HttpResponseMessage Insert(SKU item)
        {
            data.Add(item);
            var response = Request.CreateResponse<SKU>(HttpStatusCode.Created, item);

            return response;
        }
        //PUT api/sku/id nao foi testado
        public HttpResponseMessage Update(int id, SKU item)
        {
            data.Remove(data.Find(p => p.parametros.idProduto == id));
            data.Add(item);
            var response = Request.CreateResponse(HttpStatusCode.OK);
            return response;
        }

        //DELETE api/sku/id 
        public HttpResponseMessage Delete(int id)
        {
            data.Remove(data.Find(p => p.parametros.idProduto == id));
            var response = Request.CreateResponse(HttpStatusCode.OK);
            return response;
        }

        //GET
        public IEnumerable<SKU> GetAllSKUs()
        {
            return data;
        }

        //GET
        public SKU Get(int id)
        {
            var result = data.Find(p => p.parametros.idProduto == id);
            return result;   
        }
    }

   
}
