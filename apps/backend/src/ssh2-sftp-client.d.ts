declare module "ssh2-sftp-client" {
  type ConnectOptions = {
    host: string;
    port?: number;
    username: string;
    password?: string;
    privateKey?: string;
    passphrase?: string;
  };

  type FileInfo = {
    type: string;
    name: string;
    size: number;
    modifyTime?: number;
    accessTime?: number;
    rights?: {
      user?: string;
      group?: string;
      other?: string;
    };
    owner?: number;
    group?: number;
  };

  export default class SftpClient {
    connect(options: ConnectOptions): Promise<void>;
    get(path: string): Promise<any>;
    list(path?: string): Promise<FileInfo[]>;
    end(): Promise<void>;
  }
}
