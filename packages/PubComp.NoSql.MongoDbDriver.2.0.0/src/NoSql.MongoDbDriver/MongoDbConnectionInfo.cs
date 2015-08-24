using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PubComp.NoSql.MongoDbDriver
{
    public class MongoDbConnectionInfo
    {
        private const string Prefix = @"mongodb://";
        public string Host { get; set; }
        public int Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string Db { get; set; }
        public IList<KeyValuePair<string, string>> Options { get; private set; }

        public string ConnectionString
        {
            get
            {
                string userPass = string.Empty;
                if (!string.IsNullOrEmpty(Username))
                {
                    userPass = Username;
                    if (!string.IsNullOrEmpty(Password))
                        userPass += ':' + Password;

                    userPass += '@';
                }

                string dbParams = string.Empty;
                if (!string.IsNullOrEmpty(Db) || Options.Any())
                {
                    dbParams = '/' + (Db ?? string.Empty);
                    if (Options.Any())
                    {
                        dbParams += '?';
                        dbParams += string.Join(";", Options.Select(opt => opt.Key + '=' + opt.Value));
                    }
                }

                var result = string.Concat(Prefix, userPass, Host, ':', Port, dbParams);
                return result;
            }
        }

        public MongoDbConnectionInfo()
        {
            this.Host = @"localhost";
            this.Port = 27017;
            this.Username = null;
            this.Password = null;
            this.Db = "admin";
            this.Options = new List<KeyValuePair<string, string>>();
        }
    }
}
