db = db.getSiblingDB('Database');

db.createCollection('teste');

db.teste.insertOne({
  nome: "Init Script",
  created_at: new Date()
});