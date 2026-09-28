// npm install --prefix /tmp/neura-cube @cubejs-backend/postgres-driver@1.6.57 pg@8.16.3
const assert = require('node:assert/strict');
const {Client}=require('pg');
const Pg=require('@cubejs-backend/postgres-driver');
const config={host:'127.0.0.1',port:Number(process.env.NEURA_TEST_PG_PORT||55433),user:'neura',password:process.env.NEURA_PG_PASSWORD,database:'neura'};
(async()=>{
 const client=new Client(config);await client.connect();
 assert.deepEqual((await client.query('SELECT $1::int AS n, $2::text AS text',[42,"x'; DROP TABLE t; --"])).rows,[{n:42,text:"x'; DROP TABLE t; --"}]);
 assert.equal((await client.query('SELECT COUNT(*) AS n FROM pg.customers')).rows[0].n,'100');
 await assert.rejects(()=>client.query('CREATE TABLE evil AS SELECT 1'));
 await client.end();
 const Driver=Pg.PostgresDriver||Pg.default||Pg;const driver=new Driver(config);
 try {
  await driver.testConnection();
  assert.deepEqual(await driver.query('SELECT $1::int AS n',[7]),[{n:7}]);
  const rows=await driver.query('SELECT c.region, SUM(o.amount) AS total FROM pg.customers c JOIN lake.orders o ON c.id=o.customer_id GROUP BY c.region ORDER BY c.region',[]);
  const expected=require('../data/expected.json').grouped;
  assert.deepEqual(rows.map(r=>[r.region,String(r.total)]),expected);
 } finally {await driver.release();}
 console.log('PASS: node-postgres bindings, read-only rejection, RLS, Cube testConnection and federated aggregate');
})().catch(e=>{console.error(e.message);process.exit(1)});
