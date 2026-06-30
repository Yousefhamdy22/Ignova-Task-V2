import { Component, inject } from '@angular/core';
import { AsyncPipe, DecimalPipe, DatePipe } from '@angular/common';
import { TelemetryService } from '../services/telemetry.service';
import { HmiWidget } from '../hmi-widget/hmi-widget';
import { TelemetryChart } from '../telemetry-chart/telemetry-chart';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [AsyncPipe, DecimalPipe, DatePipe, HmiWidget, TelemetryChart],
  templateUrl: './dashboard.html',
  styleUrl: './dashboard.scss',
})
export class Dashboard {
  private readonly telemetryService = inject(TelemetryService);

  readonly latest$ = this.telemetryService.latest$;
  readonly isConnected$ = this.telemetryService.isConnected$;
}
