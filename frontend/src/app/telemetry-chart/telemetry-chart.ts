import {
  Component,
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  DestroyRef,
  inject,
  OnInit,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { NgxChartsModule, Color, ScaleType, LegendPosition } from '@swimlane/ngx-charts';
import { TelemetryService, TelemetryData } from '../services/telemetry.service';

// Narrow type that satisfies ngx-charts without importing internal types
type ChartSeries = { name: string; value: number };
type ChartLine   = { name: string; series: ChartSeries[] };

// en-GB locale guarantees 24-hour HH:mm:ss without AM/PM — deterministic across environments
const TIME_FORMAT: Intl.DateTimeFormatOptions = {
  hour:   '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
};

function toHHmmss(isoTimestamp: string): string {
  return new Date(isoTimestamp).toLocaleTimeString('en-GB', TIME_FORMAT);
}

@Component({
  selector: 'app-telemetry-chart',
  standalone: true,
  imports: [NgxChartsModule],
  templateUrl: './telemetry-chart.html',
  styleUrl: './telemetry-chart.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TelemetryChart implements OnInit {
  private readonly destroyRef         = inject(DestroyRef);
  private readonly cdr                = inject(ChangeDetectorRef);
  private readonly telemetryService   = inject(TelemetryService);

  private readonly MAX_POINTS = 60;


  private readonly tempSeries: ChartSeries[] = [];
  private readonly humSeries:  ChartSeries[] = [];

  chartData: ChartLine[] = [
    { name: 'Temperature °C', series: this.tempSeries },
    { name: 'Humidity %',     series: this.humSeries  },
  ];

  readonly legendPosition = LegendPosition.Below;

  readonly colorScheme: Color = {
    name:       'iot',
    selectable: true,
    group:      ScaleType.Ordinal,
    domain:     ['#E24B4A', '#378ADD'],
  };

  ngOnInit(): void {

    this.telemetryService.latest$
      .pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(data => {
        this.pushPoint(data);
        this.cdr.markForCheck();
      });
  }

  private pushPoint(data: TelemetryData): void {

    const temp = parseFloat(String(data.temperature));
    const hum  = parseFloat(String(data.humidity));

    if (isNaN(temp) || isNaN(hum)) return;

    const label = toHHmmss(data.timestamp);

    this.tempSeries.push({ name: label, value: temp });
    this.humSeries.push( { name: label, value: hum  });

    if (this.tempSeries.length > this.MAX_POINTS) this.tempSeries.shift();
    if (this.humSeries.length  > this.MAX_POINTS) this.humSeries.shift();

    this.chartData = [
      { name: 'Temperature °C', series: [...this.tempSeries] },
      { name: 'Humidity %',     series: [...this.humSeries]  },
    ];
  }
}
