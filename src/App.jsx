import { useEffect, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import packageInfo from "../package.json";
import InformationModal from "./components/InformationModal";
import "./App.css";

const pad = (value) => String(value).padStart(2, "0");

function formatClock(date) {
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function formatDate(date) {
  const weekday = new Intl.DateTimeFormat("en-GB", { weekday: "long" }).format(
    date,
  );
  const day = new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(date);

  return { weekday, day };
}

function BrandIcon() {
  return (
    <span className="brandLogoSlot" aria-hidden="true">
      <img
        src="/bridging-data.png"
        alt=""
        className="brandLogoImage"
      />
    </span>
  );
}

function ToolbarIcon({ type }) {
  return (
    <span className={`toolbarGlyph toolbarGlyph-${type}`} aria-hidden="true" />
  );
}

function TopicIcon() {
  return <span className="topicGlyph" aria-hidden="true" />;
}

const emptyTopicMapping = {
  source: "",
  destination: "",
  collectData: false,
};

const initialBridgeConfig = {
  bridgeName: "",
  sourceProtocol: "mqtt",
  sourceHost: "",
  sourcePort: "1883",
  sourceClientId: "bridge-source",
  sourceUsername: "",
  sourcePassword: "",
  destinationProtocol: "mqtt",
  destinationHost: "",
  destinationPort: "1883",
  destinationClientId: "bridge-destination",
  destinationUsername: "",
  destinationPassword: "",
  topicMappings: [{ ...emptyTopicMapping }],
};

const protocolOptions = ["mqtt", "ws", "wss"];
const storageKey = "bridging-mqtt:bridges";

function createInitialBridgeConfig() {
  return {
    ...initialBridgeConfig,
    topicMappings: [{ ...emptyTopicMapping }],
  };
}

function normalizeTopicMappings(topicMappings, sourceTopic, destinationTopic) {
  if (Array.isArray(topicMappings) && topicMappings.length) {
    return topicMappings.map((mapping) => ({
      source: mapping?.source || "",
      destination: mapping?.destination || "",
      collectData: Boolean(mapping?.collectData),
    }));
  }

  return [
    {
      source: sourceTopic || "",
      destination: destinationTopic || "",
      collectData: false,
    },
  ];
}

function normalizeBridge(bridge) {
  const normalizedMappings = normalizeTopicMappings(
    bridge.topicMappings,
    bridge.sourceTopic,
    bridge.destinationTopic,
  );
  const fallbackName =
    normalizedMappings.find((mapping) => mapping.source || mapping.destination)
      ?.source ||
    normalizedMappings.find((mapping) => mapping.source || mapping.destination)
      ?.destination ||
    "Bridge";

  return {
    ...bridge,
    bridgeName: bridge.bridgeName || bridge.name || fallbackName,
    sourceProtocol: bridge.sourceProtocol || "mqtt",
    destinationProtocol: bridge.destinationProtocol || "mqtt",
    sourcePort: bridge.sourcePort || "1883",
    destinationPort: bridge.destinationPort || "1883",
    enabled: Boolean(bridge.enabled),
    connectionState: bridge.connectionState || "disconnected",
    topicMappings: normalizeTopicMappings(
      bridge.topicMappings,
      bridge.sourceTopic,
      bridge.destinationTopic,
    ),
  };
}

function formatEndpoint(protocol, host, port) {
  const hostLabel = host || "unset-host";
  const portLabel = port || "0";
  return `${protocol}://${hostLabel}:${portLabel}`;
}

function persistBridges(nextBridges) {
  window.localStorage.setItem(storageKey, JSON.stringify(nextBridges));
}

function createLogEntry(bridgeId, level, message) {
  return {
    id: `${bridgeId}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    bridgeId,
    level,
    message,
    timestamp: new Date().toLocaleTimeString("en-GB"),
  };
}

function toBridgePayload(bridge) {
  return {
    id: bridge.id,
    bridgeName: bridge.bridgeName || bridge.name || "",
    sourceProtocol: bridge.sourceProtocol,
    sourceHost: bridge.sourceHost,
    sourcePort: bridge.sourcePort,
    sourceClientId: bridge.sourceClientId,
    sourceUsername: bridge.sourceUsername,
    sourcePassword: bridge.sourcePassword,
    destinationProtocol: bridge.destinationProtocol,
    destinationHost: bridge.destinationHost,
    destinationPort: bridge.destinationPort,
    destinationClientId: bridge.destinationClientId,
    destinationUsername: bridge.destinationUsername,
    destinationPassword: bridge.destinationPassword,
    topicMappings: bridge.topicMappings,
  };
}

function App() {
  const [now, setNow] = useState(() => new Date());
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [bridgeConfig, setBridgeConfig] = useState(() =>
    createInitialBridgeConfig(),
  );
  const [bridges, setBridges] = useState([]);
  const [editingBridgeId, setEditingBridgeId] = useState(null);
  const [logs, setLogs] = useState([]);
  const [terminalFilter, setTerminalFilter] = useState("all");
  const [terminalTopicFilter, setTerminalTopicFilter] = useState("all");
  const [isAboutOpen, setIsAboutOpen] = useState(false);
  const [bridgesLoaded, setBridgesLoaded] = useState(false);
  const [listenersReady, setListenersReady] = useState(false);
  const didRestoreBridgesRef = useRef(false);
  const appVersion = packageInfo.version;
  const selectedTerminalBridge =
    terminalFilter === "all"
      ? null
      : bridges.find((bridge) => bridge.id === terminalFilter) || null;
  const terminalTopicOptions = selectedTerminalBridge
    ? Array.from(
        new Set(
          normalizeTopicMappings(selectedTerminalBridge.topicMappings)
            .map((mapping) => mapping.source.trim())
            .filter(Boolean),
        ),
      )
    : [];
  const filteredLogs = logs.filter((log) => {
    if (terminalFilter !== "all" && log.bridgeId !== terminalFilter) {
      return false;
    }

    if (terminalTopicFilter !== "all") {
      return log.message.includes(terminalTopicFilter);
    }

    return true;
  });

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      if (!raw) {
        setBridgesLoaded(true);
        return;
      }

      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        const normalized = parsed.map(normalizeBridge).map((bridge) =>
          bridge.enabled
            ? {
                ...bridge,
                connectionState: "reconnect",
              }
            : bridge,
        );
        setBridges(normalized);
        persistBridges(normalized);
      }
    } catch {
      window.localStorage.removeItem(storageKey);
    } finally {
      setBridgesLoaded(true);
    }
  }, []);

  useEffect(() => {
    let unlistenStatus;
    let unlistenLog;

    async function registerBridgeListeners() {
      unlistenStatus = await listen("bridge-status", (event) => {
        const payload = event.payload;

        setBridges((current) => {
          const next = current.map((bridge) =>
            bridge.id === payload.id
              ? {
                  ...bridge,
                  enabled: payload.enabled,
                  connectionState: payload.connectionState,
                }
              : bridge,
          );

          persistBridges(next);
          return next;
        });

        setLogs((current) => [
          createLogEntry(
            payload.id,
            payload.connectionState === "connected"
              ? "success"
              : payload.connectionState === "reconnect"
                ? "warn"
                : "error",
            payload.error ||
              `Bridge status changed to ${payload.connectionState.toUpperCase()}`,
          ),
          ...current,
        ].slice(0, 120));
      });

      unlistenLog = await listen("bridge-log", (event) => {
        const payload = event.payload;

        setLogs((current) => [
          createLogEntry(payload.id, payload.level, payload.message),
          ...current,
        ].slice(0, 120));
      });

      setListenersReady(true);
    }

    registerBridgeListeners();

    return () => {
      if (unlistenStatus) {
        unlistenStatus();
      }
      if (unlistenLog) {
        unlistenLog();
      }
    };
  }, []);

  useEffect(() => {
    if (!bridgesLoaded || !listenersReady || didRestoreBridgesRef.current) {
      return;
    }

    didRestoreBridgesRef.current = true;

    const activeBridges = bridges.filter((bridge) => bridge.enabled);
    if (!activeBridges.length) {
      return;
    }

    activeBridges.forEach((bridge) => {
      setLogs((current) => [
        createLogEntry(
          bridge.id,
          "info",
          `Name: ${bridge.bridgeName || bridge.name} | Restoring saved connection`,
        ),
        ...current,
      ].slice(0, 120));

      invoke("start_bridge", { config: toBridgePayload(bridge) }).catch((error) => {
        setBridges((current) => {
          const next = current.map((item) =>
            item.id === bridge.id
              ? {
                  ...item,
                  enabled: false,
                  connectionState: "disconnected",
                }
              : item,
          );

          persistBridges(next);
          return next;
        });

        setLogs((current) => [
          createLogEntry(
            bridge.id,
            "error",
            `Name: ${bridge.bridgeName || bridge.name} | Failed to restore connection: ${String(error)}`,
          ),
          ...current,
        ].slice(0, 120));
      });
    });
  }, [bridges, bridgesLoaded, listenersReady]);

  useEffect(() => {
    setTerminalTopicFilter("all");
  }, [terminalFilter]);

  const { weekday, day } = formatDate(now);

  function handleChange(event) {
    const { name, value } = event.target;
    setBridgeConfig((current) => ({
      ...current,
      [name]: value,
    }));
  }

  function handleTopicMappingChange(index, field, value) {
    setBridgeConfig((current) => ({
      ...current,
      topicMappings: normalizeTopicMappings(current.topicMappings).map(
        (mapping, mappingIndex) =>
          mappingIndex === index
            ? {
                ...mapping,
                [field]: value,
              }
            : mapping,
      ),
    }));
  }

  function handleTopicMappingToggle(index, field, checked) {
    setBridgeConfig((current) => ({
      ...current,
      topicMappings: normalizeTopicMappings(current.topicMappings).map(
        (mapping, mappingIndex) =>
          mappingIndex === index
            ? {
                ...mapping,
                [field]: checked,
              }
            : mapping,
      ),
    }));
  }

  function handleSaveBridge() {
    const safeTopicMappings = normalizeTopicMappings(
      bridgeConfig.topicMappings,
    );
    const bridgeName =
      bridgeConfig.bridgeName.trim() ||
      `Bridge ${bridges.length + 1}`;
    const nextBridges = editingBridgeId
      ? bridges.map((bridge) =>
          bridge.id === editingBridgeId
            ? {
                ...bridge,
                name: bridgeName,
                bridgeName,
                ...bridgeConfig,
                topicMappings: safeTopicMappings,
              }
            : bridge,
        )
      : [
          {
            id: crypto.randomUUID(),
            name: bridgeName,
            bridgeName,
            createdAt: new Date().toISOString(),
            enabled: false,
            connectionState: "disconnected",
            ...bridgeConfig,
            topicMappings: safeTopicMappings,
          },
          ...bridges,
        ];

    setBridges(nextBridges);
    persistBridges(nextBridges);
    setBridgeConfig(createInitialBridgeConfig());
    setEditingBridgeId(null);
    setIsModalOpen(false);
  }

  function handleEditBridge(bridge) {
    setBridgeConfig({
      bridgeName: bridge.bridgeName || bridge.name || "",
      sourceProtocol: bridge.sourceProtocol || "mqtt",
      sourceHost: bridge.sourceHost || "",
      sourcePort: bridge.sourcePort || "1883",
      sourceClientId: bridge.sourceClientId || "bridge-source",
      sourceUsername: bridge.sourceUsername || "",
      sourcePassword: bridge.sourcePassword || "",
      destinationProtocol: bridge.destinationProtocol || "mqtt",
      destinationHost: bridge.destinationHost || "",
      destinationPort: bridge.destinationPort || "1883",
      destinationClientId: bridge.destinationClientId || "bridge-destination",
      destinationUsername: bridge.destinationUsername || "",
      destinationPassword: bridge.destinationPassword || "",
      topicMappings: normalizeTopicMappings(
        bridge.topicMappings,
        bridge.sourceTopic,
        bridge.destinationTopic,
      ),
    });
    setEditingBridgeId(bridge.id);
    setIsModalOpen(true);
  }

  function handleAppendTopic() {
    setBridgeConfig((current) => ({
      ...current,
      topicMappings: [
        ...normalizeTopicMappings(current.topicMappings),
        { ...emptyTopicMapping },
      ],
    }));
  }

  async function handleToggleBridge(id) {
    const bridge = bridges.find((item) => item.id === id);
    if (!bridge) {
      return;
    }

    if (bridge.enabled) {
      await invoke("stop_bridge", { bridgeId: bridge.id });
      return;
    }

    const optimistic = bridges.map((item) =>
      item.id === id
        ? {
            ...item,
            enabled: true,
            connectionState: "reconnect",
          }
        : item,
    );

    setBridges(optimistic);
    persistBridges(optimistic);

    try {
      await invoke("start_bridge", { config: toBridgePayload(bridge) });
    } catch (error) {
      const failed = optimistic.map((item) =>
        item.id === id
          ? {
              ...item,
              enabled: false,
              connectionState: "disconnected",
            }
          : item,
      );

      setBridges(failed);
      persistBridges(failed);
      console.error("Failed to start bridge", error);
    }
  }

  return (
    <main className="container">
      <header className="topbar">
        <div className="topbarBrand">
          <BrandIcon />
          <h1 className="topbarTitle">BRIDGE Data</h1>
        </div>

        <div className="topbarStripe" aria-hidden="true" />

        <div className="topbarClock">
          <strong>{formatClock(now)}</strong>
          <div className="topbarDate">
            <span>{weekday}</span>
            <span>{day}</span>
          </div>
        </div>

        <div className="topbarTools">
          <button
            type="button"
            className="topbarTool topbarToolAdd"
            aria-label="Add MQTT bridge"
            onClick={() => {
              setBridgeConfig(createInitialBridgeConfig());
              setEditingBridgeId(null);
              setIsModalOpen(true);
            }}
          >
            <span className="toolbarPlus" aria-hidden="true" />
          </button>
          <button type="button" className="topbarTool" aria-label="Fullscreen">
            <ToolbarIcon type="fullscreen" />
          </button>
          <button
            type="button"
            className="topbarTool topbarToolAbout"
            aria-label="About"
            onClick={() => setIsAboutOpen(true)}
          >
            <ToolbarIcon type="about" />
          </button>
        </div>
      </header>

      <section className="mainContainer">
        <div className="mainGrid shellFrame">
          <div className="mainGridLeft">
            <div className="mainFrame">
              <div className="videoGrid">
                <div className="workspaceCanvas">
                  <div className="workspaceContent">
                    {bridges.length ? (
                      <div className="bridgeCardGrid">
                        {bridges.map((bridge) => {
                          const bridgeMappings =
                            bridge.topicMappings && bridge.topicMappings.length
                              ? bridge.topicMappings
                              : [
                                  {
                                    source: bridge.sourceTopic || "",
                                    destination: bridge.destinationTopic || "",
                                  },
                                ];
                          const topicEntriesClassName =
                            bridgeMappings.length > 2
                              ? "bridgeTopicEntries isScrollable"
                              : "bridgeTopicEntries";

                          return (
                            <article
                              key={bridge.id}
                              className={`bridgeCard ${bridge.connectionState}`}
                            >
                            <button
                              type="button"
                              className="bridgeCardHeader"
                              onClick={() => handleEditBridge(bridge)}
                            >
                              <div className="bridgeCardTitle">
                                <strong>{bridge.name}</strong>
                              </div>

                              <div className="bridgeCardControls">
                                <span
                                  className={`navIndicator ${bridge.connectionState}`}
                                  aria-label={bridge.connectionState}
                                  title={bridge.connectionState}
                                />
                                <label
                                  className="bridgeToggle"
                                  onClick={(event) => event.stopPropagation()}
                                >
                                  <input
                                    type="checkbox"
                                    checked={Boolean(bridge.enabled)}
                                    onChange={() => handleToggleBridge(bridge.id)}
                                    onClick={(event) => event.stopPropagation()}
                                  />
                                  <span
                                    className="bridgeToggleTrack"
                                    aria-hidden="true"
                                  >
                                    <span className="bridgeToggleThumb" />
                                  </span>
                                </label>
                              </div>
                            </button>

                            <div className="bridgeCardBody">
                              <div className="bridgeEndpointGrid">
                                <div className="bridgeEndpointHead">
                                  <span>Source</span>
                                  <span>Destination</span>
                                </div>
                                <div className="bridgeEndpointValues">
                                  <p>
                                    {formatEndpoint(
                                      bridge.sourceProtocol,
                                      bridge.sourceHost,
                                      bridge.sourcePort,
                                    )}
                                  </p>
                                  <p>
                                    {formatEndpoint(
                                      bridge.destinationProtocol,
                                      bridge.destinationHost,
                                      bridge.destinationPort,
                                    )}
                                  </p>
                                </div>
                              </div>

                              <div className="bridgeTopicList">
                                <div className="bridgeTopicRow">
                                  <div className="bridgeTopicItem">
                                    <span>Source</span>
                                    <div className={topicEntriesClassName}>
                                      {bridgeMappings.map((mapping, mappingIndex) => (
                                        <p
                                          key={`${bridge.id}-source-${mappingIndex}`}
                                        >
                                          {mapping.source || "-"}
                                        </p>
                                      ))}
                                    </div>
                                  </div>
                                  <div
                                    className="bridgeTopicArrow"
                                    aria-hidden="true"
                                  >
                                    <span />
                                  </div>
                                  <div className="bridgeTopicItem">
                                    <span>Destination</span>
                                    <div className={topicEntriesClassName}>
                                      {bridgeMappings.map((mapping, mappingIndex) => (
                                        <p
                                          key={`${bridge.id}-destination-${mappingIndex}`}
                                        >
                                          {mapping.destination || "-"}
                                        </p>
                                      ))}
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </div>

                            <footer
                              className="bridgeCardFooter"
                              aria-hidden="true"
                            >
                              <div className="bridgeCardFooterStripe" />
                              <div className="bridgeCardFooterDots">
                                <span className="bridgeCardFooterDot" />
                                <span className="bridgeCardFooterDot" />
                                <span className="bridgeCardFooterDot" />
                              </div>
                            </footer>
                          </article>
                          );
                        })}
                      </div>
                    ) : (
                      <div className="emptyWorkspace">
                        <span>No bridge configured</span>
                        <p>
                          Use the + button in the top-right corner to add a new
                          MQTT bridge configuration.
                        </p>
                      </div>
                    )}

                    <section className="terminalPanel">
                      <header className="terminalHeader">
                        <span>Bridge Terminal</span>
                        <div className="terminalHeaderRail" aria-hidden="true" />
                        <label className="terminalFilter" aria-label="Filter terminal logs">
                          <select
                            className="terminalFilterSelect"
                            value={terminalFilter}
                            onChange={(event) => setTerminalFilter(event.target.value)}
                          >
                            <option value="all">All</option>
                            {bridges.map((bridge) => (
                              <option key={bridge.id} value={bridge.id}>
                                {bridge.bridgeName || bridge.name}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label
                          className="terminalFilter"
                          aria-label="Filter terminal logs by source topic"
                        >
                          <select
                            className="terminalFilterSelect"
                            value={terminalTopicFilter}
                            onChange={(event) => setTerminalTopicFilter(event.target.value)}
                            disabled={terminalFilter === "all"}
                          >
                            <option value="all">All Topics</option>
                            {terminalTopicOptions.map((topic) => (
                              <option key={topic} value={topic}>
                                {topic}
                              </option>
                            ))}
                          </select>
                        </label>
                      </header>
                      <div className="terminalBody">
                        {filteredLogs.length ? (
                          filteredLogs.map((log) => (
                            <div key={log.id} className={`terminalLine ${log.level}`}>
                              <span className="terminalTimestamp">{log.timestamp}</span>
                              <span className="terminalBridge">
                                {bridges.find((bridge) => bridge.id === log.bridgeId)?.bridgeName ||
                                  log.bridgeId.slice(0, 8)}
                              </span>
                              <span className="terminalMessage">{log.message}</span>
                            </div>
                          ))
                        ) : (
                          <div className="terminalEmpty">
                            No terminal entries for the selected filter.
                          </div>
                        )}
                      </div>
                    </section>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <footer className="bottomStatusBar" aria-hidden="true">
        <div className="bottomStatusStripe" />
        <div className="bottomStatusDots">
          <span className="statusDot"></span>
          <span className="statusDot"></span>
          <span className="statusDot"></span>
        </div>
      </footer>

      {isModalOpen ? (
        <div
          className="modalOverlay"
          role="dialog"
          aria-modal="true"
          aria-labelledby="mqtt-bridge-title"
        >
          <div className="modalFrame">
            <header className="modalHeader">
              <div>
                <span className="modalEyebrow">Bridge Configuration</span>
                <h2 id="mqtt-bridge-title" className="modalTitle">
                  {editingBridgeId ? "Edit MQTT Bridge" : "MQTT Bridge"}
                </h2>
              </div>

              <div className="modalHeaderRail" aria-hidden="true" />

              <button
                type="button"
                className="modalCloseButton"
                aria-label="Close modal"
                onClick={() => setIsModalOpen(false)}
              >
                <span />
              </button>
            </header>

            <div className="modalBody">
              <section className="configPanel configPanelWide">
                <div className="panelHeading">
                  <span>Bridge Name</span>
                  <small>Name displayed on the main dashboard card</small>
                </div>

                <label className="fieldGroup fieldGroupWide">
                  <span className="fieldLabel">Name</span>
                  <input
                    className="fieldInput"
                    name="bridgeName"
                    value={bridgeConfig.bridgeName}
                    onChange={handleChange}
                    placeholder="Main bridge"
                  />
                </label>
              </section>

              <section className="configPanel">
                <div className="panelHeading">
                  <span>Source Broker</span>
                  <small>Source broker used to subscribe to topics</small>
                </div>

                <div className="formGrid">
                  <label className="fieldGroup">
                    <span className="fieldLabel">Protocol</span>
                    <select
                      className="fieldInput fieldSelect"
                      name="sourceProtocol"
                      value={bridgeConfig.sourceProtocol}
                      onChange={handleChange}
                    >
                      {protocolOptions.map((option) => (
                        <option key={option} value={option}>
                          {option.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Host</span>
                    <input
                      className="fieldInput"
                      name="sourceHost"
                      value={bridgeConfig.sourceHost}
                      onChange={handleChange}
                      placeholder="192.168.1.10"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Port</span>
                    <input
                      className="fieldInput"
                      name="sourcePort"
                      value={bridgeConfig.sourcePort}
                      onChange={handleChange}
                      placeholder="1883"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Client ID</span>
                    <input
                      className="fieldInput"
                      name="sourceClientId"
                      value={bridgeConfig.sourceClientId}
                      onChange={handleChange}
                      placeholder="bridge-source"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Username</span>
                    <input
                      className="fieldInput"
                      name="sourceUsername"
                      value={bridgeConfig.sourceUsername}
                      onChange={handleChange}
                      placeholder="optional"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Password</span>
                    <input
                      type="password"
                      className="fieldInput"
                      name="sourcePassword"
                      value={bridgeConfig.sourcePassword}
                      onChange={handleChange}
                      placeholder="optional"
                    />
                  </label>
                </div>
              </section>

              <section className="configPanel">
                <div className="panelHeading">
                  <span>Destination Broker</span>
                  <small>Destination broker used to republish payloads</small>
                </div>

                <div className="formGrid">
                  <label className="fieldGroup">
                    <span className="fieldLabel">Protocol</span>
                    <select
                      className="fieldInput fieldSelect"
                      name="destinationProtocol"
                      value={bridgeConfig.destinationProtocol}
                      onChange={handleChange}
                    >
                      {protocolOptions.map((option) => (
                        <option key={option} value={option}>
                          {option.toUpperCase()}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Host</span>
                    <input
                      className="fieldInput"
                      name="destinationHost"
                      value={bridgeConfig.destinationHost}
                      onChange={handleChange}
                      placeholder="192.168.1.20"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Port</span>
                    <input
                      className="fieldInput"
                      name="destinationPort"
                      value={bridgeConfig.destinationPort}
                      onChange={handleChange}
                      placeholder="1883"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Client ID</span>
                    <input
                      className="fieldInput"
                      name="destinationClientId"
                      value={bridgeConfig.destinationClientId}
                      onChange={handleChange}
                      placeholder="bridge-destination"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Username</span>
                    <input
                      className="fieldInput"
                      name="destinationUsername"
                      value={bridgeConfig.destinationUsername}
                      onChange={handleChange}
                      placeholder="optional"
                    />
                  </label>
                  <label className="fieldGroup">
                    <span className="fieldLabel">Password</span>
                    <input
                      type="password"
                      className="fieldInput"
                      name="destinationPassword"
                      value={bridgeConfig.destinationPassword}
                      onChange={handleChange}
                      placeholder="optional"
                    />
                  </label>
                </div>
              </section>

              <section className="configPanel configPanelWide">
                <div className="panelHeading panelHeadingRow">
                  <div>
                    <span>Topic Mapping</span>
                    <small>
                      Source topics will be forwarded to destination topics
                    </small>
                  </div>

                  <button
                    type="button"
                    className="topicActionButton"
                    aria-label="Add topic mapping"
                    onClick={handleAppendTopic}
                  >
                    <TopicIcon />
                  </button>
                </div>

                <div className="topicMappingList">
                  {normalizeTopicMappings(bridgeConfig.topicMappings).map(
                    (mapping, index) => (
                      <div key={`mapping-${index}`} className="topicMappingRow">
                        <label className="fieldGroup">
                          <span className="fieldLabel">Topic Source</span>
                          <input
                            className="fieldInput"
                            value={mapping.source}
                            onChange={(event) =>
                              handleTopicMappingChange(
                                index,
                                "source",
                                event.target.value,
                              )
                            }
                            placeholder="factory/source/#"
                          />
                        </label>

                        <div className="topicMappingArrow" aria-hidden="true">
                          <span />
                        </div>

                        <label className="fieldGroup">
                          <span className="fieldLabel">Topic Destination</span>
                          <input
                            className="fieldInput"
                            value={mapping.destination}
                            onChange={(event) =>
                              handleTopicMappingChange(
                                index,
                                "destination",
                                event.target.value,
                              )
                            }
                            placeholder="factory/destination/data"
                          />
                        </label>

                        <div className="topicMappingMeta">
                          <label className="mappingToggleRow">
                            <input
                              type="checkbox"
                              checked={Boolean(mapping.collectData)}
                              onChange={(event) =>
                                handleTopicMappingToggle(
                                  index,
                                  "collectData",
                                  event.target.checked,
                                )
                              }
                            />
                            <span>Collect data before forwarding</span>
                          </label>
                        </div>
                      </div>
                    ),
                  )}
                </div>
              </section>
            </div>

            <footer className="modalFooter">
              <div className="modalFooterRail" aria-hidden="true" />
              <div className="modalActions">
                <button
                  type="button"
                  className="secondaryAction"
                  onClick={() => setIsModalOpen(false)}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  className="primaryAction"
                  onClick={handleSaveBridge}
                >
                  {editingBridgeId ? "Update Bridge" : "Save Bridge"}
                </button>
              </div>
            </footer>
          </div>
        </div>
      ) : null}

      <InformationModal
        open={isAboutOpen}
        title="Bridge MQTT"
        eyebrow="About Software"
        onClose={() => setIsAboutOpen(false)}
        footer={
          <>
            <div className="informationModalFooterRail" aria-hidden="true" />
            <div className="informationModalFooterDots" aria-hidden="true">
              <span />
              <span />
              <span />
            </div>
            <div className="informationModalFooterMeta">
              <strong>Bridge MQTT</strong>
              <span>STAR INTI TEKNOLOGI</span>
            </div>
            <div className="informationModalFooterActions">
              <button
                type="button"
                className="informationModalButton"
                onClick={() => setIsAboutOpen(false)}
              >
                Close
              </button>
            </div>
          </>
        }
      >
        <div className="informationModalStack">
          <section className="informationCard informationCardHero">
            <h3 className="informationCardTitle">Bridge MQTT</h3>
            <p className="informationCardText">
              &copy; Bridge MQTT — A real-time MQTT-based data bridging
              application designed to connect, route, and integrate devices and
              systems efficiently and flexibly.
            </p>
          </section>

          <section className="informationCard informationCardDetails">
            <h4 className="informationSectionTitle">Star Inti Teknologi</h4>
            <p className="informationCardText">
              Copyright 2026 Star Inti Teknologi | All rights reserved &copy;
              2026. All rights reserved.
            </p>
            <p className="informationCardText">
              Developed by Radtelindo | azidhmaulana.
            </p>
            <p className="informationCardText">
              Version {appVersion} (official release)
            </p>
          </section>
        </div>
      </InformationModal>
    </main>
  );
}

export default App;
