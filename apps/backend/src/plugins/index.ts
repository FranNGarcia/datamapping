import type { Plugin, PluginContext } from "../types";

const pluginsDir: string = import.meta.dir as unknown as string;

export async function loadPlugins(ctx: PluginContext): Promise<Plugin[]> {
  const loaded: Plugin[] = [];

  const entries = await Array.fromAsync(
    new Bun.Glob("*.plugin.{ts,js}").scan({ cwd: pluginsDir })
  );

  for (const fileName of entries) {
    const mod = (await import(new URL(`./${fileName}`, import.meta.url).href)) as {
      default?: Plugin;
    };
    const plugin = mod.default;

    if (!plugin) {
      continue;
    }

    await plugin.register(ctx);
    loaded.push(plugin);
  }

  loaded.sort((a, b) => a.name.localeCompare(b.name));
  return loaded;
}
