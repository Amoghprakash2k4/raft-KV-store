package com.projects.raftkv.core;

import java.util.Optional;

/** Volatile, observable state. Emitted in Ready only when it changes (metrics, sim checkers). */
public record SoftState(Role role, long term, Optional<NodeId> leader) {}
