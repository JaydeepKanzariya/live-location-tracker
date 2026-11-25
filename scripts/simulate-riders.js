// Load / demo simulator: publishes fake riders STRAIGHT INTO KAFKA (no browser, no socket).
// They still show up on everyone's map, which proves the map is driven by the Kafka topic,
// not by sockets echoing to each other.
//
//   npm run simulate -- --riders 10 --interval 2000 --lat 19.076 --lng 72.8777
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { ensureTopic } from '../src/kafka/client.js';
import { createLocationProducer } from '../src/kafka/producer.js';
import { buildLocationEvent, buildOfflineEvent } from '../src/events/location-event.js';

const { values: args } = parseArgs({
  options: {
    riders: { type: 'string', default: '5' },
    interval: { type: 'string', default: '2000' },
    lat: { type: 'string', default: process.env.SIM_CENTER_LAT ?? '19.0760' },
    lng: { type: 'string', default: process.env.SIM_CENTER_LNG ?? '72.8777' },
  },
});

const riderCount = Number(args.riders);
const intervalMs = Number(args.interval);
const center = { lat: Number(args.lat), lng: Number(args.lng) };

// Each rider drives in a roughly straight line, turning a little every tick (~10-15 m/s).
const riders = Array.from({ length: riderCount }, (_, i) => ({
  user: { id: `sim-rider-${i + 1}`, name: `Sim Rider ${i + 1}` },
  lat: center.lat + (Math.random() - 0.5) * 0.02,
  lng: center.lng + (Math.random() - 0.5) * 0.02,
  heading: Math.random() * 360,
  speed: 10 + Math.random() * 5,
}));

function step(rider) {
  rider.heading = (rider.heading + (Math.random() - 0.5) * 40 + 360) % 360;
  const distance = rider.speed * (intervalMs / 1000);
  const rad = (rider.heading * Math.PI) / 180;
  rider.lat += (distance * Math.cos(rad)) / 111_320;
  rider.lng += (distance * Math.sin(rad)) / (111_320 * Math.cos((rider.lat * Math.PI) / 180));
}

const producer = createLocationProducer();
await ensureTopic();
await producer.connect();
console.log(`Publishing ${riderCount} simulated riders every ${intervalMs} ms around ${center.lat},${center.lng}. Ctrl+C to stop.`);

const timer = setInterval(async () => {
  await Promise.all(
    riders.map(async (rider) => {
      step(rider);
      const location = {
        eventId: randomUUID(),
        lat: rider.lat,
        lng: rider.lng,
        accuracy: 8,
        heading: rider.heading,
        speed: rider.speed,
        clientTs: Date.now(),
        source: 'simulated',
      };
      const { partition, offset } = await producer.publish(buildLocationEvent(location, rider.user, { gatewayInstance: 'simulator' }));
      console.log(`${rider.user.id} -> partition ${partition} offset ${offset}`);
    }),
  ).catch((err) => console.error('publish failed:', err.message));
}, intervalMs);

process.on('SIGINT', async () => {
  clearInterval(timer);
  await Promise.allSettled(riders.map((r) => producer.publish(buildOfflineEvent(r.user, 'simulator-stopped'))));
  await producer.disconnect();
  process.exit(0);
});
