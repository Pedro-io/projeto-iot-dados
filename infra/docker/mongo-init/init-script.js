db = db.getSiblingDB('Database');

db.createCollection('equipments');

db.equipments.insertMany([
  {
    _id: 'EQ-SP-001',
    name: 'Compressor Industrial A1',
    type: 'compressor',
    factory: {
      id: 'FAB-SP-01',
      name: 'Fábrica São Paulo',
      location: { lat: -23.5505, lng: -46.6333 }
    },
    sensors: [
      { id: 'SENS-SP-001-TEMP', type: 'temperature', range: { min: 0, max: 150 } },
      { id: 'SENS-SP-001-VIBR', type: 'vibration', range: { min: 0, max: 100 } }
    ],
    maintenance_schedule: 'monthly',
    installed_at: ISODate('2023-06-15T00:00:00Z'),
    status: 'active'
  },
  {
    _id: 'EQ-RJ-001',
    name: 'Bomba Hidráulica B2',
    type: 'pump',
    factory: {
      id: 'FAB-RJ-01',
      name: 'Fábrica Rio de Janeiro',
      location: { lat: -22.9068, lng: -43.1729 }
    },
    sensors: [
      { id: 'SENS-RJ-001-PRES', type: 'pressure', range: { min: 0, max: 20 } },
      { id: 'SENS-RJ-001-HUM', type: 'humidity', range: { min: 0, max: 100 } }
    ],
    maintenance_schedule: 'weekly',
    installed_at: ISODate('2023-07-12T00:00:00Z'),
    status: 'active'
  },
  {
    _id: 'EQ-MG-001',
    name: 'Motor Elétrico C3',
    type: 'motor',
    factory: {
      id: 'FAB-MG-01',
      name: 'Fábrica Belo Horizonte',
      location: { lat: -19.9167, lng: -43.9345 }
    },
    sensors: [
      { id: 'SENS-MG-001-CURR', type: 'current', range: { min: 0, max: 100 } }
    ],
    maintenance_schedule: 'monthly',
    installed_at: ISODate('2023-08-01T00:00:00Z'),
    status: 'active'
  }
]);

db.equipments.createIndex({ 'factory.id': 1, status: 1 });
db.equipments.createIndex({ 'sensors.id': 1 });