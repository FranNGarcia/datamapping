import { getCustomerAggregateFieldPaths, getCustomerAggregatePrimaryKeyFieldPaths } from "../domain/customerFields";
import { getProductAggregateFieldPaths, getProductAggregatePrimaryKeyFieldPaths } from "../domain/productFields";
import type { Plugin } from "../types";

const plugin: Plugin = {
  name: "domain",
  manifest: {
    name: "domain",
    kind: "utility",
    title: "Domain"
  },
  register(ctx) {
    ctx.registerRoute({
      method: "GET",
      path: "/api/domain/customer/fields",
      handler: () => {
        return json({
          ok: true,
          aggregate: "Customer",
          fields: getCustomerAggregateFieldPaths(),
          primaryKeys: getCustomerAggregatePrimaryKeyFieldPaths()
        });
      }
    });

    ctx.registerRoute({
      method: "GET",
      path: "/api/domain/product/fields",
      handler: () => {
        return json({
          ok: true,
          aggregate: "Product",
          fields: getProductAggregateFieldPaths(),
          primaryKeys: getProductAggregatePrimaryKeyFieldPaths()
        });
      }
    });
  }
};

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" }
  });
}

export default plugin;
