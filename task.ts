import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import type { Event } from '@tak-ps/etl';
import { Feature } from '@tak-ps/node-cot'
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import Schema from '@openaddresses/batch-schema';

// eslint-disable-next-line @typescript-eslint/no-unused-vars --  Fetch with an additional Response.typed(TypeBox Object) definition
import { fetch } from '@tak-ps/etl';

/**
 * The Input Schema contains the environment object that will be requested via the CloudTAK UI
 * It should be a valid TypeBox object - https://github.com/sinclairzx81/typebox
 */
const InputSchema = Type.Object({
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

/**
 * The Output Schema contains the known properties that will be returned on the
 * GeoJSON Feature in the .properties.metdata object
 */
const OutputSchema = Type.Object({})

const WebhookBody = Type.Object({
    'serial number': Type.String({ description: 'Device serial number, used as the feature callsign and UID' }),
    'date/time': Type.String({ description: 'Event timestamp in "YYYY-MM-DD HH:mm:ss" format' }),
    'Latitude': Type.Number(),
    'Longitude': Type.Number(),
    'Event': Type.String({ description: 'Event code' }),
    'solar power': Type.String({ description: 'Solar power reading' }),
    'Speed': Type.String({ description: 'Speed in knots' }),
    'Heading': Type.Number({ description: 'Heading in degrees' }),
});

export default class Task extends ETL {
    static name = 'etl-ensurity'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule, InvocationType.Webhook ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return InputSchema;
            } else {
                return OutputSchema;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const features: Static<typeof Feature.InputFeature>[] = [];

        // Get things here and convert them to GeoJSON Feature Collections
        // That conform to the node-cot Feature properties spec
        // https://github.com/dfpc-coe/node-CoT/

        const fc: Static<typeof Feature.InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: features
        }

        await this.submit(fc);
    }

    static async webhooks(
        schema: Schema,
        task: Task
    ): Promise<void> {
        schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'Ensurity vehicle data webhook',
            params: Type.Object({
                webhookid: Type.String()
            }),
            body: WebhookBody,
            res: Type.Object({
                status: Type.Number(),
                message: Type.String()
            })
        }, async (req, res) => {
            const body = req.body as Static<typeof WebhookBody>;

            const time = new Date(body['date/time'].replace(' ', 'T') + 'Z');
            const stale = new Date(time.getTime() + 5 * 60 * 1000);
            const timeISO = time.toISOString();
            const staleISO = stale.toISOString();

            const feature: Static<typeof Feature.InputFeature> = {
                id: body['serial number'],
                type: 'Feature',
                properties: {
                    callsign: body['serial number'],
                    type: 'a-f-G-U-C',
                    how: 'm-g',
                    time: timeISO,
                    start: timeISO,
                    stale: staleISO,
                    center: [body['Longitude'], body['Latitude'], 0],
                    track: {
                        course: String(body['Heading']),
                        speed: body['Speed'],
                    },
                    metadata: {
                        event: body['Event'],
                        solarPower: body['solar power'],
                    },
                },
                geometry: {
                    type: 'Point',
                    coordinates: [body['Longitude'], body['Latitude'], 0],
                },
            };

            const fc: Static<typeof Feature.InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: [feature],
            };

            await task.submit(fc);

            return res.json({
                status: 200,
                message: 'Webhook payload received'
            });
        });
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}, context?: object) {
    return await internal(new Task(import.meta.url), event, context);
}

