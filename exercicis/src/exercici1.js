const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
const winston = require('winston');
require('dotenv').config();

// Logger que utilizaremos para imprimir por pantalla y volcar en fichero log
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => `${timestamp} ${level}: ${message}`)
    ),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: './data/logs/exercici1.log' })
    ],
});

// Ruta al fitxer XML
const xmlFilePath = path.join(__dirname, '../../data/Posts.xml');

// Funció per llegir i analitzar el fitxer XML

async function parseXMLFile(filePath) {
  try {
    const xmlData = fs.readFileSync(filePath, 'utf-8');
    const parser = new xml2js.Parser({ 
      explicitArray: false,
      mergeAttrs: true
    });

    return new Promise((resolve, reject) => {
      parser.parseString(xmlData, (err, result) => {
        if (err) {
          return reject(err);
        }

        try {
          // Validación básica de estructura
          if (!result.posts || !result.posts.row) {
            return resolve([]);
          }

          // Asegurar array
          let rows = result.posts.row;
          if (!Array.isArray(rows)) {
            rows = [rows];
          }

          // Filtramos para tener sólo los posts de tipo pregunta
          const questionsOnly = rows.filter(r => r.PostTypeId === '1');

          // Mapear al formato requerido
          const mapped = questionsOnly.map(r => ({
            question: {
              Id: r.Id,
              PostTypeId: r.PostTypeId,
              AcceptedAnswerId: r.AcceptedAnswerId || null,
              CreationDate: r.CreationDate,
              Score: r.Score,
              ViewCount: r.ViewCount || '0',
              Body: r.Body,
              OwnerUserId: r.OwnerUserId,
              LastActivityDate: r.LastActivityDate,
              Title: r.Title || '',
              Tags: r.Tags || '',
              AnswerCount: r.AnswerCount || '0',
              CommentCount: r.CommentCount || '0',
              ContentLicense: r.ContentLicense
            }
          }));

          // Ordenar por ViewCount DESC y coger los 10000 primeros
          const top10000 = mapped
            .sort((a, b) => Number(b.question.ViewCount) - Number(a.question.ViewCount))
            .slice(0, 10000);

          resolve(top10000);

        } catch (e) {
          reject(e);
        }
      });
    });

  } catch (error) {
    logger.error('Error llegint o analitzant el fitxer XML:', error);
    throw error;
  }
}

async function loadDataToMongoDB() {
  // Configuració de la connexió a MongoDB
  const uri = process.env.MONGODB_URI || 'mongodb://root:password@127.0.0.1:27017/?authSource=admin';
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    logger.info('Connectat a MongoDB');
    
    const database = client.db('boardgames_db');
    const collection = database.collection('boardgames');
    
    // Leer y procesar el XML
    logger.info('Llegint el fitxer XML...');
    const xmlData = await parseXMLFile(xmlFilePath);
    
    // Eliminar dades existents (opcional)
    logger.info('Eliminant dades existents...');
    await collection.deleteMany({});
    
    // Inserir les noves dades
    logger.info('Inserint dades a MongoDB...');
    const result = await collection.insertMany(xmlData);
    
    logger.info(`${result.insertedCount} documents inserits correctament.`);
    logger.info('Dades carregades amb èxit!');
    
  } catch (error) {
    logger.error('Error carregant les dades a MongoDB:', error);
  } finally {
    await client.close();
    logger.info('Connexió a MongoDB tancada');
  }
}

// Ejercución del Ejercicio 1
(async () => {
  try {
    // const data = await parseXMLFile(xmlFilePath);

    // logger.info('Total resultados:', data.length);
    // logger.info('Primer elemento:', data[0]);
    // logger.info('Último elemento:', data[data.length - 1]);

    loadDataToMongoDB();

  } catch (error) {
    logger.error(error);
  }
})();