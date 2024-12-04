using M2M.SiaSplittingTestingTool;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

var host = builder.Build();

DatabaseManager.DatabaseManagerAppsettingsConfiguration(host.Services.GetRequiredService<IConfiguration>());

host.Run();
