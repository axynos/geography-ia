const dayjs = require('dayjs')
const isBetween = require('dayjs/plugin/isBetween')
const utc = require('dayjs/plugin/utc')
dayjs.extend(isBetween)
dayjs.extend(utc)

const { MongoClient } = require('mongodb')
const fs = require('fs')
const fsp = fs.promises
const path = require('path')

// List of station IDs included for the analysis of each area
const areaStations = {
  centre: [41, 12, 49, 56, 43, 45, 32],
  nonCentre: [59, 34, 36, 52, 27]
}
const dbUrl = 'mongodb+srv://tartusmartbikes:<password>@cluster0-deda2.gcp.mongodb.net/test?retryWrites=true&w=majority'
const utcOffset = 3

const main = async _ => {
  //await cacheData()
  await loadCachedData()
  await processData()
}

const processData = async _ => {
  //                   YYYY-MM-DD HH:MM        UTC offset
  const start = dayjs('2020-04-29T05:00:00.000+03:00')
  const end   = dayjs('2020-04-30T00:00:00.000+03:00')
  const chunkSize = 0.5 // hours
  const data = await loadCachedData()
  const chunks = generateChunks(start, end, chunkSize)
  const keys = Object.keys(data)

  const volatilityMap = chunks.map(chunk => {
    const timestamp = chunk.start

    const relevantKeys = keys.filter(key => {
      return dayjs(key).isBetween(chunk.start, chunk.end)
    })

    const relevantSamples = relevantKeys.map(key => {
      return data[key]
    })

    const volatility = calculateVolatility(relevantSamples, chunkSize)

    return {
      timestamp: timestamp.toISOString(),
      humanTimestamp: timestamp.utcOffset(utcOffset).format('MMM DD, YYYY HH:mm'),
      volatility: volatility,
      //relevantSamples: relevantSamples
    }
  })

  // Write the sample data point to file.
  fs.writeFile(`${__dirname}/data/volatilityMap.json`, JSON.stringify(volatilityMap, null, 2), (err) => {
    if (err) throw err;
    console.log(`VolatilityMap saved.`);
  })


}

// Simple comparison for dayjs dates to be able to sort an array of them
const dayjsCompare = (a, b) => {
  const aDate = dayjs(a)
  const bDate = dayjs(b)

  // A is less than B
  if (aDate.isBefore(bDate)) {
    return -1
  }

  // B is greater than A
  if (bDate.isBefore(aDate)) {
    return 1
  }

  // A and B are equal
  if (aDate.isSame(bDate)) {
    return 0
  }
}

// Duration input is given in hours
const calculateVolatility = (samples, duration) => {
  const sortedSamples = samples.sort(dayjsCompare)
  const volatilityIterator = makeVolatilityIterator(0, sortedSamples)

  let volatility = volatilityIterator.next()
  while (!volatility.done) {
    volatility = volatilityIterator.next()
  }

  const averageVolatilityOverTime = {
    centre: (volatility.value.centre / areaStations.centre.length / duration).toFixed(3),
    nonCentre: (volatility.value.nonCentre / areaStations.nonCentre.length / duration).toFixed(3)
  }

  console.log(averageVolatilityOverTime)


  return averageVolatilityOverTime
}

const makeVolatilityIterator = (start=0, samples) => {
  let nextIndex = start
  const step = 1

  const stations = samples[nextIndex].stations
  let accumulator = {
    centre: 0,
    nonCentre: 0
  }

  const iterator = {
    next: _ => {
      // Continue if next sample is available.
      if (samples[nextIndex+1]) {
        const currentSample = samples[nextIndex]
        const nextSample = samples[nextIndex+1]

        const changes = calculateChanges(currentSample, nextSample)

        nextIndex += step
        accumulator = {
          centre: accumulator.centre + changes.centre,
          nonCentre: accumulator.nonCentre + changes.nonCentre
        }

        return { value: nextIndex, done: false }
      }
      accumulator = {
        centre: accumulator.centre,
        nonCentre: accumulator.nonCentre
      }
      return { value: accumulator, done: true}
    }
  }

  return iterator
}

const calculateChanges = (currentSample, nextSample) => {
  const currentStations = currentSample.stations
  const nextStations = nextSample.stations

  const centreStations = {
    current: currentStations.filter(station => areaStations.centre.some((id) => id == station.id)),
    next: nextStations.filter(station => areaStations.centre.some((id) => id == station.id))
  }

  const nonCentreStations = {
    current: currentStations.filter(station => areaStations.nonCentre.some((id) => id == station.id)),
    next: nextStations.filter(station => areaStations.nonCentre.some((id) => id == station.id))
  }

  const centreChanges = centreStations.current.map((station, index) => {
    return Math.abs(station.currentCapacity - centreStations.next[index].currentCapacity)
  })

  const nonCentreChanges = nonCentreStations.current.map((station, index) => {
    return Math.abs(station.currentCapacity - nonCentreStations.next[index].currentCapacity)
  })

  // Add all the changes together
  const centreChangesSum = centreChanges.reduce((a, b) => a + b)
  const nonCentreChangesSum = nonCentreChanges.reduce((a, b) => a + b)

  const changes = {
    centre: centreChangesSum,
    nonCentre: nonCentreChangesSum
  }

  return changes
}

const generateChunks = (start, end, chunkSize=1) => {
  let chunks = []
  const step  = chunkSize
  const chunkCount = end.diff(start, 'hour')

  const chunkIterator = makeChunkIterator(start, end, step)

  let result = chunkIterator.next()
  while (!result.done) {
    chunks = chunks.concat(result.value)
    result = chunkIterator.next()
  }

  return chunks
}

const makeChunkIterator = (start, end, step) => {
  let nextIndex = start

  const iterator = {
    next: _ => {
      if (nextIndex.isBefore(end)) {
        const chunk = {
          start: nextIndex,
          end: nextIndex.add(step, 'hour')
        }

        nextIndex = nextIndex.add(step, 'hour')
        return { value: chunk, done: false }
      }
      return { value: undefined, done: true}
    }
  }

  return iterator
}

const loadCachedData = async _ => {
  const basePath = `${__dirname}/data/clean`
  let samples = {}

  const fileNames = await fsp.readdir(basePath)

  // Waits for all the files to load into the samples structure before continuing
  await Promise.all(fileNames.map(async fileName => {
    if (path.extname(fileName) !== '.json') return

    try {
      const file = await fsp.readFile(path.join(basePath, fileName))
      //console.log(`Loading file with fileName ${fileName}`)
      const data = JSON.parse(file)

      // Adds the data to the samples list with the timestamp as the key for filtering.
      samples[data.timestamp] = data
    } catch (e) {
      console.log(`Error in file: ${fileName}`);
      console.error(e)
    }
  }))

  return samples
}

// Load all documents from the mongodb database and save the cleaned up versions to files.
const cacheData = async _ => {
  const client = await MongoClient.connect(dbUrl, { useNewUrlParser: true, useUnifiedTopology: true })
      .catch(err => { console.log(err) })

   if (!client) {
      return;
   }

   try {

      const db = client.db('geography-ia')
      let collection = db.collection('tartu-smart-bikes')

      // Get all values from latest to oldest
      const cursor = await collection.find({}, { sort: { _id: -1 }})

      await cursor.forEach(doc => {
        const timestamp = dayjs(doc.timestamp)
        const stations = doc.stations.map(station => {
          return {
            name: station.name,
            id: station.serial_number,
            maxCapacity: station.stocking_full,
            currentCapacity: station.total_locked_cycle_count,
            freeCapacity: station.free_dockes
          }
        })

        const dataPoint = {
          timestamp: timestamp,
          stations: stations
        }

        // Write the sample data point to file.
        fs.writeFile(`${__dirname}/data/clean/${timestamp.utcOffset(utcOffset).toISOString()}.json`, JSON.stringify(dataPoint, null, 2), (err) => {
          if (err) throw err;
          console.log(`${timestamp.toISOString()} saved.`);
        })
      })

  } catch (err) {
      console.log(err)
  } finally {
      client.close()
  }
}

main()
