using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;

namespace Reeve.Cluster.AspNetCore;

internal sealed class ResponseHeaderSnapshot(IHeaderDictionary headers)
{
    public IHeaderDictionary Headers { get; } = headers;

    public static void Capture(HttpContext context)
    {
        var headers = new HeaderDictionary();
        foreach (var (name, value) in context.Response.Headers)
        {
            headers[name] = value;
        }

        context.Features.Set(new ResponseHeaderSnapshot(headers));
    }

    public static void Restore(HttpContext context)
    {
        if (context.Features.Get<ResponseHeaderSnapshot>() is not { } snapshot)
        {
            return;
        }

        foreach (var (name, value) in snapshot.Headers)
        {
            context.Response.Headers[name] = value;
        }
    }
}
