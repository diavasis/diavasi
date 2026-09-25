export interface Record {
  recordId: number;
  payload: Buffer;
}

export interface Batch {
  batchId: number;
  records: Record[];
}

export interface Report {
  recordIds: number[];
  batchIds: number[];
}

export interface ConsumeOptions {
  addr: string;
  ca: string;
  token: string;
  groupId: string;
  consumerId: string;
  maxInFlight?: number;
  haltAfterAcks?: number;
  expectRecords?: number;
  protoPath?: string;
}

export class ProtocolError extends Error {
  code: number;
  constructor(code: number, message: string);
}

export class CallError extends Error {
  status: string;
  constructor(status: string, message: string);
}

export function consume(options: ConsumeOptions): Promise<Report>;
