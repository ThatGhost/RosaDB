using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace RosaDB.Client.TUI.Persistence
{
    public class SavedQueriesManager
    {
        private static readonly string AppDataPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        private static readonly string RosaDBDataPath = Path.Combine(AppDataPath, "RosaDB");
        private static readonly string SavedQueriesFilePath = Path.Combine(RosaDBDataPath, "saved_queries.json");

        public static event Action QueriesChanged;

        public SavedQueriesManager()
        {
            if (!Directory.Exists(RosaDBDataPath))
            {
                Directory.CreateDirectory(RosaDBDataPath);
            }
        }

        public List<string> GetQueries()
        {
            if (!File.Exists(SavedQueriesFilePath))
            {
                return new List<string>();
            }

            var json = File.ReadAllText(SavedQueriesFilePath);
            return JsonConvert.DeserializeObject<List<string>>(json) ?? new List<string>();
        }

        public void SaveQuery(string query)
        {
            var queries = GetQueries();
            if (!queries.Contains(query))
            {
                queries.Add(query);
                SaveChanges(queries);
            }
        }

        public void DeleteQuery(string query)
        {
            var queries = GetQueries();
            queries.Remove(query);
            SaveChanges(queries);
        }

        private void SaveChanges(List<string> queries)
        {
            var json = JsonConvert.SerializeObject(queries, Formatting.Indented);
            File.WriteAllText(SavedQueriesFilePath, json);
            QueriesChanged?.Invoke();
        }
    }
}
