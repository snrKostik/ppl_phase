const std = @import("std");
const Io = std.Io;
const net = Io.net;

const curl = @import("curl");
const zhtml = @import("zhtml");

const URL = "https://pepeland.net";
// const debug = true;
const json_template =
    \\{{
++ "\n\t" ++
    \\"status": {d},
++ "\n\t" ++
    \\"current_phase": "{s}",
++ "\n\t" ++
    \\"cached_phase": "{s}"
    \\}}
;
pub fn main(init: std.process.Init) !void {
    var gpa = std.heap.SafeAllocator.init(std.heap.page_allocator, .{});
    defer if (gpa.deinit() > 0) @panic("\n\n leaked WOW \n\n");
    const alloc = gpa.allocator();

    const io = init.io;

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_file_writer: Io.File.Writer = .init(.stdout(), io, &stdout_buffer);
    const out = &stdout_file_writer.interface;

    // setting up http server for getting json
    const address = try net.IpAddress.parse("127.0.0.1", 1111);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    while (true) {
        const stream = try server.accept(io);
        handleConnection(io, alloc, out, stream);
    }

    try out.flush();
}
fn handleConnection(io: Io, alloc: std.mem.Allocator, out: *Io.Writer, stream: net.Stream) void {
    defer stream.close(io);

    var read_buf: [4096]u8 = undefined;
    var write_buf: [4096]u8 = undefined;
    var connection_reader = stream.reader(io, &read_buf);
    var connection_writer = stream.writer(io, &write_buf);

    var server = std.http.Server.init(
        &connection_reader.interface,
        &connection_writer.interface,
    );
    while (server.reader.state == .ready) {
        var request = server.receiveHead() catch |err| switch (err) {
            error.HttpConnectionClosing => return,
            else => {
                std.debug.print("failed to read request: {t}\n", .{err});
                return;
            },
        };

        route(io, alloc, out, &request) catch |err| {
            std.debug.print("failed to serve {s}: {t}", .{ request.head.target, err });
        };
    }
}

fn route(io: Io, alloc: std.mem.Allocator, out: *Io.Writer, request: *std.http.Server.Request) !void {
    // const method = request.head.method;
    const path = request.head.target;

    if (std.mem.eql(u8, path, "/ppl_phase")) {
        const json = try fetchPhase(io, alloc, out);
        defer alloc.free(json);
        try respondJson(request, .ok, json);
    }
}

fn respondJson(request: *std.http.Server.Request, status: std.http.Status, body: []const u8) !void {
    try request.respond(body, .{
        .status = status,
        .extra_headers = &.{
            .{
                .name = "content-type",
                .value = "application/json",
            },
        },
    });
}

var i: u64 = 0;
fn fetchPhase(io: Io, alloc: std.mem.Allocator, out: *Io.Writer) ![]u8 {
    const cwd = std.Io.Dir.cwd();
    const output_json = try cwd.createFile(io, "foo.json", .{});
    defer output_json.close(io);

    const cached_phase = try cwd.createFile(io, "cached_phase.txt", .{});
    defer cached_phase.close(io);
    //
    var ca_bundle = try curl.allocCABundle(alloc, io);
    defer ca_bundle.deinit(alloc);
    var easy = try curl.Easy.init(.{
        .ca_bundle = ca_bundle,
    });
    defer easy.deinit();

    // while (true) {
    defer i += 1;
    // const start_time = Io.Timestamp.toMilliseconds(.now(io, .real));
    var html_buf: [256 * 1024]u8 = undefined;
    var html_writer = std.Io.Writer.fixed(&html_buf);

    try easy.setFollowLocation(true);
    const resp = try easy.fetch(URL, .{ .writer = &html_writer });
    const html = html_writer.buffered();
    // std.debug.print("BODY: {s}\nSTATUS: {d}\n", .{ html, resp.status_code });
    try out.print("STATUS: {d}\n", .{resp.status_code});

    const html_parse_options: zhtml.ParseOptions = .{};

    var doc = try html_parse_options.parse(alloc, html);
    defer doc.deinit();

    var phase_div = doc.query("div#season-preparation-details > div");
    defer phase_div.deinit();

    const phase_node = (try phase_div.next()) orelse return error.UnexpectedResult;

    const normalized = try phase_node.innerTextWithOptions(alloc, .{});
    defer normalized.free(alloc);

    const current_phase = normalized.value;

    if (resp.status_code == 200) {
        _ = try cached_phase.writePositionalAll(io, current_phase, 0);
    }
    const cached_phase_val = try cwd.readFileAlloc(
        io,
        "cached_phase.txt",
        alloc,
        .unlimited,
    );
    defer alloc.free(cached_phase_val);

    try out.print("{d} CURRENT PHASE: {s}\n", .{ i, current_phase });

    const json_final = try std.fmt.allocPrint(alloc, json_template, .{
        resp.status_code,
        current_phase,
        cached_phase_val,
    });
    _ = try output_json.writePositionalAll(io, json_final, 0);

    // const end_time = Io.Timestamp.toMilliseconds(.now(io, .real));
    // const elapsed = end_time - start_time;
    // const minute_ms = 60_000;
    // const interval = if (debug) minute_ms / 60 else minute_ms;
    // if (elapsed < interval) {
    //     try Io.sleep(io, .fromMilliseconds(interval - elapsed), .real);
    // }
    try out.flush();
    return json_final;
}
