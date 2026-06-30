import {
  Component,
  ChangeDetectionStrategy,
  computed,
  input,
} from '@angular/core';
import { DecimalPipe } from '@angular/common';

@Component({
  selector: 'app-hmi-widget',
  standalone: true,
  imports: [DecimalPipe],
  templateUrl: './hmi-widget.html',
  styleUrl: './hmi-widget.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class HmiWidget {

  readonly temperature = input<string | number>(0);


  readonly parsedTemp = computed(() =>
    parseFloat(String(this.temperature())) || 0
  );

  readonly color = computed((): string => {
    const t = this.parsedTemp();
    if (t < 40) return '#378ADD';
    if (t <= 70) return '#EF9F27';
    return '#E24B4A';
  });

  readonly status = computed((): 'Normal' | 'Warning' | 'Critical' => {
    const t = this.parsedTemp();
    if (t < 40) return 'Normal';
    if (t <= 70) return 'Warning';
    return 'Critical';
  });

  readonly fillHeight = computed((): number => {
    const clamped = Math.min(Math.max(this.parsedTemp(), 0), 100);
    return (clamped / 100) * 130;
  });

  readonly fillY = computed((): number => 155 - this.fillHeight());
}
