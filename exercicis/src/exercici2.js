require('dotenv').config();
const { MongoClient } = require('mongodb');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');

async function runQueries() {
    const uri = process.env.MONGODB_URI || "mongodb://root:password@127.0.0.1:27017/?authSource=admin";
    const client = new MongoClient(uri);

    try {
        // Conexión MongoDB
        await client.connect();
        const db = client.db("boardgames_db");
        const collection = db.collection("boardgames");
        console.log('Connectat a MongoDB');

        const outDir = path.join(__dirname, './data/out');
        if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

        // Calcular media de ViewCounts
        console.log("Calculant mitjana de ViewCounts...");
        const stats = await collection.aggregate([
            {
                $project: {
                    ViewCountNum: { $toDouble: "$question.ViewCount" }
                }
            },
            {
                $group: { _id: null, avgViewCount: { $avg: "$ViewCountNum" } }
            }
        ]).toArray();

        const avg = stats.length > 0 && stats[0].avgViewCount !== null ? stats[0].avgViewCount : 0;
        console.log(`Mitjana de ViewCount: ${avg.toFixed(2)}`);

        // Query 1 --> Cuántos posts con ViewCount mayor que la media anterior
        const questionsAboveAvg = await collection.aggregate([
        {
            $addFields: {
            ViewCountNum: { $toDouble: "$question.ViewCount" }
            }
        },
        {
            $match: {
            ViewCountNum: { $gt: avg }
            }
        }
        ]).toArray();

        console.log(`Consulta 1: ${questionsAboveAvg.length} preguntes trobades.`);
        generarPDF(path.join(outDir, 'informe1.pdf'), 
                    "Informe 1: Preguntes amb ViewCount sobre la mitjana", 
                    questionsAboveAvg);

        // Query 2 --> Posts cuyo título contenga alguna de las siguientes combinaciones
        const keywords = ["pug", "wig", "yak", "nap", "jig", "mug", "zap", "gag", "oaf", "elf"];
        const regex = new RegExp(keywords.join('|'), 'i');
        const questionsWithKeywords = await collection.find({
            "question.Title": { $regex: regex }
        }).toArray();
        console.log(`Consulta 2: ${questionsWithKeywords.length} preguntes trobades.`);
        generarPDF(path.join(outDir, 'informe2.pdf'), "Informe 2: Preguntes amb paraules clau al títol", questionsWithKeywords);

    } catch (err) {
        console.error("Error executant consultes:", err);
    } finally {
        await client.close();
    }
}

// Genera informes PDF usando resultados de las query
function generarPDF(filePath, title, data) {
    const doc = new PDFDocument();
    doc.pipe(fs.createWriteStream(filePath));

    doc.fontSize(18).text(title, { align: 'center' });
    doc.moveDown();
    doc.fontSize(12).text(`Total de resultats: ${data.length}`);
    doc.moveDown();

    data.forEach((item, index) => {
        doc.fontSize(10).text(`${index + 1}. ${item.question.Title}`);
        doc.moveDown(0.5);

        if (index === 500) {
            doc.fillColor('red').text("... (llista truncada per límit d'espai)");
            return;
        }
    });

    doc.end();
    console.log(`Arxiu generat: ${path.basename(filePath)}`);
}

runQueries();