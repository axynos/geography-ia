import axios from 'axios'
import dayjs from 'dayjs'
import mongodb from 'mongodb'
const { MongoClient } = mongodb

const apiUrl = 'https://ratas.tartu.ee/stations/stations/'
const dbUrl = 'mongodb+srv://tartusmartbikes:<password>@cluster0-deda2.gcp.mongodb.net/test?retryWrites=true&w=majority'

export default async (req, res) => {
  const timestamp = dayjs().format('DD/MM/YYYY HH:MM:ss')
  const stations = await requestStations()

  try {
    await saveData(stations, timestamp)

    const status = 200
    const response = {
      timestamp: timestamp,
      stations: stations.length
    }

    res.status(status).json(response)
  } catch (e) {
    console.error(e)

    const status = 500
    const error = {
      timestamp: timestamp,
      error: e
    }

    res.status(500).json({error})
  }
}

const requestStations = async _ => {
  const response = await axios.get(apiUrl)
  const stations = response.data

  return stations
}

const saveData = async (stations, timestamp) => {
  const client = await MongoClient.connect(dbUrl, { useNewUrlParser: true })
       .catch(err => { console.log(err) })

   if (!client) {
       return;
   }

   try {

       const db = client.db('geography-ia')
       let collection = db.collection('tartu-smart-bikes')

       let res = await collection.insertOne({
         timestamp: new Date(),
         stations: stations
       })

       console.log(`Saved stations data at ${timestamp}.`)
       //console.log(res)

   } catch (err) {
       console.log(err)
   } finally {
       client.close()
   }
}
