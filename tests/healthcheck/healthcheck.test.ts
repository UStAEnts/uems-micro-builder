import getHealthcheck, { _clearHealthcheck, HealthcheckServer, launchCheck, tryApplyTrait } from "../../src/healthcheck/Healthcheck";
import * as http from "http";
import axios from "axios";

describe('Healthcheck.ts', () => {

    beforeEach(async () => {
        _clearHealthcheck();
    })

    afterEach(async () => {
        try {
            await getHealthcheck().close();
        } catch (e) {
            console.warn('Failed to close healthcheck server', e);
        }
    })

    it('should host by default on 7777', async () => {
        const server = await launchCheck([], () => 'healthy');
        console.log('launched');
        await axios.get('http://localhost:7777/invalid', { timeout: 500 }).then((data) => {
            expect(false).toBeTruthy();
        }).catch((err) => {
            expect(err).toHaveProperty(['response', 'status'], 404);
            return server.close();
        });
    });

    it('should mark undefined declared traits with _undefined', async () => {
        const server = await launchCheck(['test trait'], () => 'healthy');

        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(data.data).toHaveProperty('test trait', '_undefined');
            return server.close();
        }).catch(() => {
            expect(false).toBeTruthy();
        });
    });

    it('should apply the validator to responses', async () => {
        const server = await launchCheck(['test trait'], () => 'unhealthy');

        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(false).toBeTruthy();
        }).catch((err) => {
            expect(err).toHaveProperty(['response', 'status'], 500);
            expect(err.response.data).toHaveProperty('status', 'unhealthy');
            return server.close();
        });
    });

    it('should handle validators throwing errors', async () => {
        const server = await launchCheck(['test trait'], () => {
            throw new Error('invalid');
        });

        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(false).toBeTruthy();
        }).catch((err) => {
            expect(err).toHaveProperty(['response', 'status'], 500);
            expect(err.response.data).toHaveProperty('status', 'unhealthy');
            return server.close();
        });
    });

    it('should return traits in the body', async () => {
        const server = await launchCheck(['test trait'], () => 'healthy');
        server.trait('something', 'else');

        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(data.data).toHaveProperty('test trait', '_undefined');
            expect(data.data).toHaveProperty('something', 'else');
            return server.close();
        }).catch((e) => {
            expect(false).toBeTruthy();
        });
    });

    it('try setting traits should work as expected', async () => {
        tryApplyTrait('before', 'set');

        const server = await launchCheck([], () => 'healthy');
        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(Object.keys(data.data)).toEqual(['status']);
        }).catch((e) => {
            expect(false).toBeTruthy();
        });

        tryApplyTrait('after', 'set');

        await axios.get('http://localhost:7777/healthcheck', { timeout: 500 }).then((data) => {
            expect(Object.keys(data.data).sort()).toEqual(['after', 'status'].sort());
            expect(data.data).toHaveProperty('after', 'set');
        }).catch((e) => {
            console.log(e);
            expect(false).toBeTruthy();
        });

        await server.close();
    });

})
