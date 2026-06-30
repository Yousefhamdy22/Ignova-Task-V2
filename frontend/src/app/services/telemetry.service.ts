import { Injectable } from '@angular/core';
import { webSocket } from 'rxjs/webSocket';
import { Observable, BehaviorSubject } from 'rxjs';
import { bufferTime, filter, share, map, retry } from 'rxjs/operators';

export interface TelemetryData {
  tenantID: string;
  deviceID: string;
  temperature: number;
  humidity: number;
  timestamp: string;
}

// In production, derive this from your auth service / route params / JWT claims.
// It must match a tenantID registered in MongoDB.
const WS_TENANT_ID = 'tenant_A_123';

@Injectable({ providedIn: 'root' })
export class TelemetryService {
  private readonly _connected$ = new BehaviorSubject<boolean>(false);
  readonly isConnected$ = this._connected$.asObservable();

  readonly telemetry$: Observable<TelemetryData[]>;
  readonly latest$: Observable<TelemetryData>;

  constructor() {
    const socket$ = webSocket<any>({
      // Backend now requires tenantID query param — only messages for this
      // tenant will be pushed to this WebSocket connection.
      url: `ws://localhost:8080?tenantID=${WS_TENANT_ID}`,
      openObserver:  { next: () => this._connected$.next(true) },
      closeObserver: { next: () => this._connected$.next(false) },
    });

    this.telemetry$ = socket$.pipe(
      map(raw => ({
        tenantID:    raw.tenantID,
        deviceID:    raw.deviceID,
        temperature: parseFloat(raw.temperature),
        humidity:    parseFloat(raw.humidity),
        timestamp:   raw.timestamp,
      } as TelemetryData)),
      retry({ delay: 3000 }),
      bufferTime(100),
      filter(batch => batch.length > 0),
      share()
    );

    this.latest$ = this.telemetry$.pipe(
      map(batch => batch[batch.length - 1])
    );
  }
}
