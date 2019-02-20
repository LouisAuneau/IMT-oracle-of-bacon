const mongodb = require('mongodb');
const csv = require('csv-parser');
const fs = require('fs');

const MongoClient = mongodb.MongoClient;
const mongoUrl = 'mongodb://localhost:27017/oracle_of_bacon';

const insertActors = (db, callback) => {
    const collection = db.collection('actors');
    const actors = [];
    fs.createReadStream('actors.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
        .on('data', data => {
            actors.push({
                "imdb_id": data.imdb_id,
                "name": data.name,
                "birth_date": data.birth_date
            });
        })
        // A la fin on créé l'ensemble des acteurs dans MongoDB
        .on('end', () => {
            collection.insertMany(actors, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, db) => {
    insertActors(db, result => {
        console.log(`${result.insertedCount} actors inserted`);
        addIndexes(db);
        db.close();
    });
});

/**
 * Ajout d'un index sur le nom des acteurs
 */
function addIndexes(db){
    db.collection('actors').createIndex( 
        { name : "text" }
    );
}