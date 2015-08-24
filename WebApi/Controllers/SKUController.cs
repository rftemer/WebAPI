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
using MongoDB.Driver.Wrappers;
using System.IO;
using WebApi.App_Start;



namespace WebApi.Controllers
{
    public class SKUController : ApiController
    {
      
      
        //POST api/sku/?{...
        public HttpResponseMessage Insert(SKU item)
        {
            SyncDataBase.Data.Add(item);
            var response = Request.CreateResponse<SKU>(HttpStatusCode.OK, item);

            return response;
        }
        //PUT api/sku/id?{...}
        public HttpResponseMessage Update(int id, SKU item)
        {
            SyncDataBase.Data.Remove(SyncDataBase.Data.Find(p => p.parametros.idProduto == id));
            SyncDataBase.Data.Add(item);
            var response = Request.CreateResponse(HttpStatusCode.OK);
            return response;
        }

        //DELETE api/sku/id 
        public HttpResponseMessage Delete(int id)
        {
            SyncDataBase.Data.Remove(SyncDataBase.Data.Find(p => p.parametros.idProduto == id));
            var response = Request.CreateResponse(HttpStatusCode.OK);
            return response;
        }

        //GET
        public IEnumerable<SKU> GetAllSKUs()
        {
            return SyncDataBase.Data;
        }

        //GET
        public SKU Get(int id)
        {
            var result = SyncDataBase.Data.Find(p => p.parametros.idProduto == id);
            return result;   
        }
    }

   
}
