const serverless = require('serverless-http');
const express = require('express');
const multer = require('multer');
const Papa = require('papaparse');
const { DBFFile } = require('dbffile');
const fs = require('fs');
const iconv = require('iconv-lite');
const cors = require('cors');

const app = express();
const upload = multer({ dest: '/tmp/' });

// Налаштування CORS
const corsOptions = {
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    preflightContinue: false,
    optionsSuccessStatus: 204
};

app.use(cors(corsOptions));
app.use(express.urlencoded({ extended: true }));

app.get('/', (req, res) => {
    res.send('CSV to DBF Converter is running on Lambda.');
});

app.post('/convert', upload.single('csvFile'), async (req, res) => {
    const csvFilePath = req.file.path;

    const csvData = iconv.decode(fs.readFileSync(csvFilePath), 'utf8');

    const parsedData = Papa.parse(csvData, { header: true });

    fs.unlinkSync(csvFilePath);

    const dbfFilePath = '/tmp/output.dbf';

    if (fs.existsSync(dbfFilePath)) {
        fs.unlinkSync(dbfFilePath);
    }

    const fields = parsedData.meta.fields.map(field => {
        const fieldType = req.body[`fieldType_${field.trim()}`] || 'C';
        const fieldSize = parseInt(req.body[`fieldSize_${field.trim()}`], 10) || 255;
        return {
            name: field.trim().substring(0, 10),
            type: fieldType,
            size: fieldSize
        };
    });

    const records = parsedData.data.map(record => {
        const obj = {};
        parsedData.meta.fields.forEach(field => {
            const fieldName = field.trim().substring(0, 10);
            const fieldType = req.body[`fieldType_${field.trim()}`];
            let value = record[field];
            if (fieldType === 'N') {
                value = parseFloat(value);
                if (isNaN(value)) value = 0;
            } else if (fieldType === 'L') {
                value = value.toLowerCase() === 'true' || value === '1';
            } else if (fieldType === 'D') {
                value = new Date(value);
                if (isNaN(value.getTime())) value = new Date(0);
            }
            obj[fieldName] = value;
        });
        return obj;
    });

    const dbf = await DBFFile.create(dbfFilePath, fields, { encoding: 'CP866' });
    await dbf.appendRecords(records);

    res.download(dbfFilePath, 'output.dbf', () => {
        fs.unlinkSync(dbfFilePath);
    });
});

module.exports.handler = serverless(app);
