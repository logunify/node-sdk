import axios from "axios";
import winston from "winston";

export type Options = {
    apiKey: string,
    ssl?: boolean,
    host?: string,
    path?: string,
    port?: number,
    batchInterval?: number,
    minBatchSize?: number,
    enableDebugLogging?: boolean,
    maxBulkSize?: number,
    maxAttempts?: number,
    logLevel?: string,
}
const logFormat = winston.format.printf(
    ({ level, message, label, timestamp, event }) => {
        const baseFormat = `${timestamp} [${label}] ${level}: ${message}`;
        if (event != null) {
            return `${baseFormat}, data: ${JSON.stringify(event)}`;
        } else {
            return baseFormat;
        }
    }
);

export interface LogUnifyEvent {
    getSchemaName: () => string,
    getProjectName: () => string,
    serialize: () => Uint8Array,
}

export default class LogUnifyLogger {
    private static instance: LogUnifyLogger;

    private ssl: boolean = false;
    private host: string = 'localhost';
    private path: string = 'api/events/_bulk';
    private port: number = 80;
    private batchInterval: number = 5000;
    private minBatchSize: number = 10;
    private maxBulkSize: number = 50;
    private maxAttempts: number = 3;

    private events: LogUnifyEvent[] = [];
    private isSendingEvents: boolean = false;
    private lastScheduled: number = -1;
    private apiKey: string;
    private logger;

    public static setup(options: Options) {
        if (this.instance == null) {
            this.instance = new LogUnifyLogger(options);
        } else {
            this.instance.configure(options);
        }
        return this.instance;
    }

    public static get() {
        if (this.instance == null) {
            throw new Error('Logger is not initialized, please call setupLogger() to initialize the logger.');
        }
        return this.instance;
    }

    private constructor(options: Options) {
        this.logger = winston.createLogger({
            level: options.logLevel || 'info',
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.label({ label: 'LogUnify' }),
                logFormat,
            ),
            transports: [
                new winston.transports.Console(),
            ]
        });

        this.apiKey = options.apiKey;
        this.configure(options);
    }

    private configure(options: Options) {
        this.apiKey = options.apiKey;
        this.ssl = options.ssl || this.ssl;
        this.host = options.host || this.host;
        this.path = options.path || this.path;
        this.batchInterval = options.batchInterval || this.batchInterval;
        this.minBatchSize = options.minBatchSize || this.minBatchSize;
        this.maxBulkSize = options.maxBulkSize || this.maxBulkSize;
        if (options.maxAttempts != null && options.maxAttempts > 0) {
            this.maxAttempts = this.maxAttempts;
        }
        this.port = options.port || this.port;

        return this;
    }

    log(event: any) {
        this.events.push(event);
        this.logger.debug("Logged event", { event: event.toObject() });

        if (this.lastScheduled == -1 || Date.now() / 1000 - this.lastScheduled > this.batchInterval) {
            this.logger.debug(`Scheduled a batch sent in ${this.batchInterval / 1000} seconds.`);
            this.lastScheduled = Date.now() / 1000;
            setTimeout(() => {
                this.sendEvents();
            }, this.batchInterval);
        }
        if (this.events.length === this.minBatchSize) {
            this.logger.debug(`Scheduled an immediate batch sent.`);
            // max batch count is reached, send all messages to endpoint
            this.sendEvents();
        }
    }

    private async sendEvents() {
        if (this.isSendingEvents) {
            // skip if there's other batch request running
            return;
        }
        this.logger.debug("Started batch event sending.");
        this.isSendingEvents = true;


        while (this.events.length > 0) {
            const bulk = this.events.slice(0, this.maxBulkSize);
            let attemptsLeft = this.maxAttempts;
            while (attemptsLeft-- > 0) {
                if (await this.makeRequest(bulk)) {
                    this.events.splice(0, bulk.length);
                    break;
                }
            }

            if (attemptsLeft === 0) {
                return false;
            }
        }

        this.isSendingEvents = false;
    }

    private async makeRequest(events: LogUnifyEvent[]) {
        const postingEvents = events.map(event => ({
            serializedEvent: Buffer.from(event.serialize()).toString('base64'),
            // TODO: update when we have the methods generateds
            schemaName: 'UserActivity',
            // projectName: event.getProjectName(),
        }));

        try {
            await axios.post(
                `${this.ssl ? 'https' : 'http'}://${this.host}:${this.port}/${this.path}`,
                JSON.stringify({ events: postingEvents }),
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Auth-Token': this.apiKey
                    }
                }
            );
            this.logger.debug(`Successfully sent ${events.length} events.`);
            return true;
        } catch (e) {
            this.logger.debug(`Failed to send ${events.length} events with error: ${e}`);
            return false;
        }
    }
}
