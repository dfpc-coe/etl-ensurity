import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import Schema from '@openaddresses/batch-schema';

const InputSchema = Type.Object({
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

const WebhookBody = Type.Object({
    'serial number': Type.Union([Type.Number(), Type.String()], { description: 'Device serial number, used as the feature UID' }),
    'imei': Type.Optional(Type.String({ description: 'Device IMEI' })),
    'date/time': Type.String({ description: 'Event timestamp in "YYYY-MM-DD HH:mm:ss" format' }),
    'Latitude': Type.Number(),
    'Longitude': Type.Number(),
    'Event': Type.Union([Type.Number(), Type.String()], { description: 'Event code' }),
    'solar power': Type.Union([Type.Number(), Type.String()], { description: 'Solar power reading' }),
    'Speed': Type.Union([Type.Number(), Type.String()], { description: 'Speed in knots' }),
    'Heading': Type.Number({ description: 'Heading in degrees' }),
    'in1': Type.Optional(Type.Boolean()),
    'in2': Type.Optional(Type.Boolean()),
    'in3': Type.Optional(Type.Boolean()),
    'in4': Type.Optional(Type.Boolean()),
    'in5': Type.Optional(Type.Boolean()),
    'in6': Type.Optional(Type.Boolean()),
    'magnetAbsent': Type.Optional(Type.Boolean()),
    'name': Type.Optional(Type.String({ description: 'Device name, used as the default callsign' })),
});

/**
 * Metadata written to each feature; mirrors the raw webhook body field names
 */
const OutputSchema = Type.Omit(WebhookBody, ['serial number', 'date/time', 'Latitude', 'Longitude', 'Speed', 'Heading']);

type InputFeature = Static<typeof Feature.InputFeature>;

function isObject(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null;
}

function isFeature(value: unknown): value is InputFeature {
    return isObject(value)
        && value.type === 'Feature'
        && isObject(value.properties)
        && isObject(value.geometry);
}

function isFeatureList(value: unknown): value is { total: number; items: InputFeature[] } {
    return isObject(value)
        && typeof value.total === 'number'
        && Array.isArray(value.items)
        && value.items.every(isFeature);
}

function stringify(value: unknown): string {
    if (typeof value === 'string') return value;
    if (typeof value === 'object' && value !== null) return JSON.stringify(value);
    return String(value);
}

export default class Task extends ETL {
    static name = 'etl-ensurity'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule, InvocationType.Webhook ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow !== DataFlowType.Incoming) return Type.Object({});

        return type === SchemaType.Input ? InputSchema : OutputSchema;
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();
        const env = await this.env(InputSchema);
        const now = new Date();

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        };

        const limit = 100;
        let page = 0;
        let total = Infinity;

        while (fc.features.length < total) {
            const url = new URL(`/api/connection/${layer.connection}/feature`, this.etl.api);
            url.searchParams.set('layer', String(layer.id));
            url.searchParams.set('format', 'geojson');
            url.searchParams.set('download', 'false');
            url.searchParams.set('limit', String(limit));
            url.searchParams.set('page', String(page));
            url.searchParams.set('sort', 'id');
            url.searchParams.set('order', 'asc');
            url.searchParams.set('filter', '');

            const res: unknown = await this.fetch(url);
            if (!isFeatureList(res)) throw new Error('Unexpected Feature List response');

            total = res.total;

            for (const feat of res.items) {
                const staleValue = feat.properties.stale;
                const stale = staleValue === undefined || staleValue === '' || staleValue === 0 ? null : new Date(staleValue);
                if (stale && !Number.isNaN(stale.getTime()) && stale < now) continue;

                feat.properties.archived = false;
                fc.features.push(feat);
            }

            if (res.items.length < limit) break;
            page++;
        }

        if (env.DEBUG) {
            console.log(JSON.stringify(fc, null, 2));
        }

        await this.submit(fc, {
            archive: false
        });
    }

    static async webhooks(
        schema: Schema,
        task: ETL
    ): Promise<void> {
        await schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'Ensurity vehicle data webhook',
            params: Type.Object({
                webhookid: Type.String()
            }),
            body: Type.Any(),
            res: Type.Object({
                status: Type.Number(),
                message: Type.String()
            })
        }, (req, res, next) => {
            const body: unknown = req.body;

            console.error(body);

            const receive = async (): Promise<void> => {
                try {
                    await Task.receive(task, body);

                    res.json({
                        status: 200,
                        message: 'Webhook payload received'
                    });
                } catch (err) {
                    next(err);
                }
            };

            void receive();
        });
    }

    static async receive(
        task: ETL,
        body: unknown
    ): Promise<void> {
        if (!isObject(body)) throw new TypeError('Webhook payload is not an object');

        const datetime = body['date/time'];
        if (typeof datetime !== 'string') throw new TypeError('Webhook payload is missing date/time');

        const { Longitude, Latitude } = body;
        if (typeof Longitude !== 'number' || typeof Latitude !== 'number') {
            throw new TypeError('Webhook payload is missing Longitude/Latitude');
        }

        const time = new Date(datetime.replace(' ', 'T') + 'Z');
        const stale = new Date(time.getTime() + 5 * 60 * 1000);
        const timeISO = time.toISOString();
        const staleISO = stale.toISOString();

        const serial = stringify(body['serial number']);
        const callsign = (typeof body.name === 'string' && body.name) || serial;

        const feature: InputFeature = {
            id: serial,
            type: 'Feature',
            properties: {
                callsign,
                type: 'a-h-G',
                how: 'm-g',
                time: timeISO,
                start: timeISO,
                stale: staleISO,
                center: [Longitude, Latitude, 0],
                track: {
                    course: stringify(body.Heading),
                    speed: stringify(body.Speed),
                },
                metadata: {
                    name: body.name,
                    imei: body.imei,
                    Event: body.Event,
                    'solar power': body['solar power'],
                    in1: body.in1,
                    in2: body.in2,
                    in3: body.in3,
                    in4: body.in4,
                    in5: body.in5,
                    in6: body.in6,
                    magnetAbsent: body.magnetAbsent,
                },
            },
            geometry: {
                type: 'Point',
                coordinates: [Longitude, Latitude, 0],
            },
        };

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: [feature],
        };

        await task.submit(fc);
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}, context?: object): ReturnType<typeof internal> {
    return internal(new Task(import.meta.url), event, context);
}

